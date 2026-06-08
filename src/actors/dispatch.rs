use std::sync::{Arc, Mutex};

use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use signal_frame::{BatchErrorClassification, NonEmpty, Reply as FrameReply, Request, SubReply};
use signal_sema::{SemaOperation, ToSemaOperation};
use signal_spirit::{
    EffectEmitted, Operation as WorkingOperation, OperationKind, OperationReceived,
    RecordObservation, Reply as WorkingReply, RequestUnimplemented, UnimplementedReason,
};
use triad_runtime::{
    ContinuationExhausted, NextStep, NexusAction as TriadNexusAction, Runner, RunnerEngines,
};

use crate::observation::{
    Command, Effect, RecordIdentifierObservation, RecordSubscriptionObservation,
};
use crate::store::StampedEntry;
use crate::{Error, Result};

use super::classifier;
use super::clock;
use super::pipeline::{FramePipelineReply, PipelineReply};
use super::reply;
use super::state;
use super::store;
use super::subscription;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct DispatchPhase {
    classifier: ActorRef<classifier::ClassifierPlane>,
    clock: ActorRef<clock::ClockPlane>,
    store: ActorRef<store::RecordStore>,
    state: ActorRef<state::StatePlane>,
    subscription: ActorRef<subscription::SubscriptionPlane>,
    reply: ActorRef<reply::ReplyShaper>,
}

#[derive(Clone)]
pub struct Arguments {
    pub classifier: ActorRef<classifier::ClassifierPlane>,
    pub clock: ActorRef<clock::ClockPlane>,
    pub store: ActorRef<store::RecordStore>,
    pub state: ActorRef<state::StatePlane>,
    pub subscription: ActorRef<subscription::SubscriptionPlane>,
    pub reply: ActorRef<reply::ReplyShaper>,
}

pub struct RouteRequest {
    pub request: WorkingOperation,
    pub trace: ActorTrace,
}

pub struct RouteFrameRequest {
    pub request: Request<WorkingOperation>,
    pub trace: ActorTrace,
}

#[derive(Clone)]
struct SharedTrace {
    trace: Arc<Mutex<ActorTrace>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpiritCommandEffect {
    command: Command,
    effect: Effect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpiritSemaWriteInput {
    Execute(Command),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpiritSemaReadInput {
    Execute(Command),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpiritSemaOutput {
    Completed(SpiritCommandEffect),
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpiritNexusWork {
    SignalArrived(WorkingOperation),
    SemaCompleted(SpiritSemaOutput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpiritNexusAction {
    CommandSemaWrite(SpiritSemaWriteInput),
    CommandSemaRead(SpiritSemaReadInput),
    ReplyToSignal(WorkingReply),
}

struct SpiritRuntimeEngines {
    classifier: ActorRef<classifier::ClassifierPlane>,
    clock: ActorRef<clock::ClockPlane>,
    store: ActorRef<store::RecordStore>,
    state: ActorRef<state::StatePlane>,
    subscription: ActorRef<subscription::SubscriptionPlane>,
    reply: ActorRef<reply::ReplyShaper>,
    trace: SharedTrace,
    last_error: Option<Error>,
}

impl DispatchPhase {
    fn new(
        classifier: ActorRef<classifier::ClassifierPlane>,
        clock: ActorRef<clock::ClockPlane>,
        store: ActorRef<store::RecordStore>,
        state: ActorRef<state::StatePlane>,
        subscription: ActorRef<subscription::SubscriptionPlane>,
        reply: ActorRef<reply::ReplyShaper>,
    ) -> Self {
        Self {
            classifier,
            clock,
            store,
            state,
            subscription,
            reply,
        }
    }

    async fn route(&self, request: WorkingOperation, trace: ActorTrace) -> Result<PipelineReply> {
        self.route_frame(Request::from_payload(request), trace)
            .await?
            .into_single_pipeline_reply()
    }

    async fn route_frame(
        &self,
        request: Request<WorkingOperation>,
        trace: ActorTrace,
    ) -> Result<FramePipelineReply> {
        let trace = SharedTrace::new(trace);
        trace.record(TraceNode::DISPATCH_PHASE, TraceAction::MessageReceived);
        let operation_count = request.payloads().len();
        if operation_count != 1 {
            let error = Error::UnsupportedAtomicBatch { operation_count };
            let reply = Self::batch_aborted_reply(&error, operation_count);
            return Ok(FramePipelineReply::new(reply, trace.snapshot()));
        }
        let operation = request.payloads.into_head();
        let mut engines = SpiritRuntimeEngines::new(
            self.classifier.clone(),
            self.clock.clone(),
            self.store.clone(),
            self.state.clone(),
            self.subscription.clone(),
            self.reply.clone(),
            trace.clone(),
        );
        let signal_reply = Runner::default()
            .drive(&mut engines, SpiritNexusWork::SignalArrived(operation))
            .await;
        let reply = match engines.take_last_error() {
            Some(error) => Self::batch_aborted_reply(&error, operation_count),
            None => FrameReply::committed(NonEmpty::single(SubReply::Ok(signal_reply))),
        };
        Ok(FramePipelineReply::new(reply, trace.snapshot()))
    }

    fn batch_aborted_reply(error: &Error, operation_count: usize) -> FrameReply<WorkingReply> {
        let mut per_operation = NonEmpty::single(SubReply::Invalidated);
        for _ in 1..operation_count {
            per_operation.push(SubReply::Invalidated);
        }
        FrameReply::batch_aborted(
            error.batch_failure_reason(),
            error.retry_classification(),
            error.commit_status(),
            per_operation,
        )
    }
}

impl SharedTrace {
    fn new(trace: ActorTrace) -> Self {
        Self {
            trace: Arc::new(Mutex::new(trace)),
        }
    }

    fn snapshot(&self) -> ActorTrace {
        self.trace
            .lock()
            .expect("persona-spirit trace mutex poisoned")
            .clone()
    }

    fn replace(&self, trace: ActorTrace) {
        *self
            .trace
            .lock()
            .expect("persona-spirit trace mutex poisoned") = trace;
    }

    fn record(&self, node: TraceNode, action: TraceAction) {
        self.trace
            .lock()
            .expect("persona-spirit trace mutex poisoned")
            .record(node, action);
    }
}

impl SpiritCommandEffect {
    fn new(command: Command, effect: Effect) -> Self {
        Self { command, effect }
    }

    fn command(&self) -> &Command {
        &self.command
    }

    fn effect(&self) -> &Effect {
        &self.effect
    }
}

impl triad_runtime::SemaWriteInput for SpiritSemaWriteInput {}

impl triad_runtime::SemaReadInput for SpiritSemaReadInput {}

impl triad_runtime::NexusWork for SpiritNexusWork {}

impl TriadNexusAction for SpiritNexusAction {
    type Reply = WorkingReply;
    type SemaWrite = SpiritSemaWriteInput;
    type SemaRead = SpiritSemaReadInput;
    type Effect = std::convert::Infallible;
    type Work = SpiritNexusWork;

    fn into_next_step(self) -> triad_runtime::NexusActionNextStep<Self> {
        match self {
            Self::CommandSemaWrite(input) => NextStep::SemaWrite(input),
            Self::CommandSemaRead(input) => NextStep::SemaRead(input),
            Self::ReplyToSignal(reply) => NextStep::Reply(reply),
        }
    }
}

impl SpiritRuntimeEngines {
    fn new(
        classifier: ActorRef<classifier::ClassifierPlane>,
        clock: ActorRef<clock::ClockPlane>,
        store: ActorRef<store::RecordStore>,
        state: ActorRef<state::StatePlane>,
        subscription: ActorRef<subscription::SubscriptionPlane>,
        reply: ActorRef<reply::ReplyShaper>,
        trace: SharedTrace,
    ) -> Self {
        Self {
            classifier,
            clock,
            store,
            state,
            subscription,
            reply,
            trace,
            last_error: None,
        }
    }

    fn take_last_error(&mut self) -> Option<Error> {
        self.last_error.take()
    }

    fn action_for_signal(&self, operation: WorkingOperation) -> SpiritNexusAction {
        let Some(command) = Command::from_request(operation.clone()) else {
            return SpiritNexusAction::ReplyToSignal(Self::unimplemented_reply(&operation));
        };
        self.publish_operation_received(&operation);
        Self::action_for_command(command)
    }

    fn action_for_command(command: Command) -> SpiritNexusAction {
        match command.to_sema_operation() {
            SemaOperation::Match => {
                SpiritNexusAction::CommandSemaRead(SpiritSemaReadInput::Execute(command))
            }
            _ => SpiritNexusAction::CommandSemaWrite(SpiritSemaWriteInput::Execute(command)),
        }
    }

    fn action_for_sema_output(&self, output: SpiritSemaOutput) -> SpiritNexusAction {
        match output {
            SpiritSemaOutput::Completed(effect) => {
                self.publish_effect_emitted(&effect);
                SpiritNexusAction::ReplyToSignal(effect.effect().clone().into_reply())
            }
            SpiritSemaOutput::Rejected => {
                SpiritNexusAction::ReplyToSignal(Self::unimplemented_reply_for_engine_failure())
            }
        }
    }

    fn unimplemented_reply(_operation: &WorkingOperation) -> WorkingReply {
        WorkingReply::RequestUnimplemented(RequestUnimplemented {
            reason: UnimplementedReason::IntegrationNotLanded,
        })
    }

    fn unimplemented_reply_for_engine_failure() -> WorkingReply {
        WorkingReply::RequestUnimplemented(RequestUnimplemented {
            reason: UnimplementedReason::IntegrationNotLanded,
        })
    }

    async fn execute_command(&self, command: Command) -> Result<SpiritCommandEffect> {
        let reply = match command.clone() {
            Command::ClassifyStatement(statement) => self.classify_statement(statement).await?,
            Command::AssertEntry(entry) => self.capture_entry(entry).await?,
            Command::RemoveRecord(identifier) => self.remove_entry(identifier).await?,
            Command::ChangeCertainty(change) => self.change_certainty(change).await?,
            Command::ChangeRecord(change) => self.change_record(change).await?,
            Command::CollectRemovalCandidates(collection) => {
                self.collect_removal_candidates(collection).await?
            }
            Command::ReadRecords(observation) => self.observe_records(observation).await?,
            Command::ReadRecordIdentifiers(query) => self.observe_record_identifiers(query).await?,
            Command::ReadTopics => self.observe_topics().await?,
            Command::ReadState => self.observe_state().await?,
            Command::ReadQuestions => self.observe_questions().await?,
            Command::OpenStateSubscription => self.subscribe_state().await?,
            Command::OpenRecordSubscription(subscription) => {
                self.subscribe_records(subscription).await?
            }
            Command::CloseStateSubscription(token) => {
                self.retract_state_subscription(token).await?
            }
            Command::CloseRecordSubscription(token) => {
                self.retract_record_subscription(token).await?
            }
            Command::OpenObserverSubscription(_filter) => {
                self.shape_unimplemented(OperationKind::Tap).await?
            }
            Command::CloseObserverSubscription(_token) => {
                self.shape_unimplemented(OperationKind::Untap).await?
            }
        };
        Ok(SpiritCommandEffect::new(command, Effect::from_reply(reply)))
    }

    async fn capture_entry(&self, entry: signal_spirit::Entry) -> Result<WorkingReply> {
        let entry = self.stamp_entry(entry).await?;
        self.capture_stamped_entry(entry).await
    }

    async fn stamp_entry(&self, entry: signal_spirit::Entry) -> Result<StampedEntry> {
        let trace = self.trace.snapshot();
        let stamped = self
            .clock
            .ask(clock::StampEntry { entry, trace })
            .await
            .map_err(Self::clock_send_error)?;
        self.trace.replace(stamped.trace);
        Ok(stamped.entry)
    }

    async fn capture_stamped_entry(&self, entry: StampedEntry) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::CaptureEntry { entry, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn remove_entry(
        &self,
        identifier: signal_spirit::RecordIdentifier,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::RemoveEntry { identifier, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn change_certainty(
        &self,
        change: signal_spirit::CertaintyChange,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::ChangeCertainty { change, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn change_record(&self, change: signal_spirit::RecordChange) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::ChangeRecord { change, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn collect_removal_candidates(
        &self,
        collection: signal_spirit::RemovalCandidateCollection,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::CollectRemovalCandidates { collection, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn classify_statement(
        &self,
        statement: signal_spirit::Statement,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let classified = self
            .classifier
            .ask(classifier::ClassifyStatement { statement, trace })
            .await
            .map_err(Self::classifier_send_error)?;
        self.trace.replace(classified.trace);
        self.capture_entry(classified.entry).await
    }

    async fn observe_records(&self, observation: RecordObservation) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::ObserveRecords { observation, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn observe_record_identifiers(
        &self,
        observation: RecordIdentifierObservation,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::ObserveRecordIdentifiers { observation, trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn observe_topics(&self) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .store
            .ask(store::ObserveTopics { trace })
            .await
            .map_err(Self::store_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn observe_state(&self) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .state
            .ask(state::ObserveState { trace })
            .await
            .map_err(Self::state_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn observe_questions(&self) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .state
            .ask(state::ObserveQuestions { trace })
            .await
            .map_err(Self::state_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn subscribe_state(&self) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let snapshot = self
            .state
            .ask(state::ReadStateSnapshot { trace })
            .await
            .map_err(Self::state_send_error)?;
        self.trace.replace(snapshot.trace.clone());
        let pipeline = self
            .subscription
            .ask(subscription::OpenStateSubscription {
                snapshot: snapshot.state,
                trace: snapshot.trace,
            })
            .await
            .map_err(Self::subscription_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn subscribe_records(
        &self,
        observation: RecordSubscriptionObservation,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let snapshot = self
            .store
            .ask(store::ReadRecordSnapshot {
                observation: observation.clone(),
                trace,
            })
            .await
            .map_err(Self::store_send_error)?;
        self.trace.replace(snapshot.trace.clone());
        let pipeline = self
            .subscription
            .ask(subscription::OpenRecordSubscription {
                subscription: observation.subscription,
                snapshot: snapshot.records,
                trace: snapshot.trace,
            })
            .await
            .map_err(Self::subscription_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn retract_state_subscription(
        &self,
        token: signal_spirit::StateSubscriptionToken,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .subscription
            .ask(subscription::RetractStateSubscription { token, trace })
            .await
            .map_err(Self::subscription_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn retract_record_subscription(
        &self,
        token: signal_spirit::RecordSubscriptionToken,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .subscription
            .ask(subscription::RetractRecordSubscription { token, trace })
            .await
            .map_err(Self::subscription_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    async fn shape_unimplemented(&self, operation: OperationKind) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let pipeline = self
            .reply
            .ask(reply::ShapeUnimplemented { operation, trace })
            .await
            .map_err(Self::reply_send_error)?;
        let (reply, trace) = pipeline.into_parts();
        self.trace.replace(trace);
        Ok(reply)
    }

    fn store_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn state_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }

    fn classifier_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }

    fn clock_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }

    fn subscription_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }

    fn reply_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }

    fn publish_operation_received(&self, operation: &WorkingOperation) {
        let _event = OperationReceived {
            operation: operation.kind(),
        };
        self.trace
            .record(TraceNode::NEXUS_RUNNER, TraceAction::OperationReceived);
    }

    fn publish_effect_emitted(&self, effect: &SpiritCommandEffect) {
        let _event = EffectEmitted {
            operation: effect.command().operation_kind(),
            outcome: effect.effect().outcome(),
        };
        self.trace
            .record(TraceNode::SEMA_OBSERVER, TraceAction::ObservationProjected);
    }
}

impl RunnerEngines for SpiritRuntimeEngines {
    type Reply = WorkingReply;
    type SemaWrite = SpiritSemaWriteInput;
    type SemaRead = SpiritSemaReadInput;
    type Effect = std::convert::Infallible;
    type Work = SpiritNexusWork;

    fn decide_next_step(
        &mut self,
        work: Self::Work,
    ) -> NextStep<Self::Reply, Self::SemaWrite, Self::SemaRead, Self::Effect, Self::Work> {
        let action = match work {
            SpiritNexusWork::SignalArrived(operation) => self.action_for_signal(operation),
            SpiritNexusWork::SemaCompleted(output) => self.action_for_sema_output(output),
        };
        TriadNexusAction::into_next_step(action)
    }

    async fn apply_sema_write(&mut self, input: Self::SemaWrite) -> Self::Work {
        let output = match input {
            SpiritSemaWriteInput::Execute(command) => match self.execute_command(command).await {
                Ok(effect) => SpiritSemaOutput::Completed(effect),
                Err(error) => {
                    self.last_error = Some(error);
                    SpiritSemaOutput::Rejected
                }
            },
        };
        SpiritNexusWork::SemaCompleted(output)
    }

    async fn observe_sema_read(&mut self, input: Self::SemaRead) -> Self::Work {
        let output = match input {
            SpiritSemaReadInput::Execute(command) => match self.execute_command(command).await {
                Ok(effect) => SpiritSemaOutput::Completed(effect),
                Err(error) => {
                    self.last_error = Some(error);
                    SpiritSemaOutput::Rejected
                }
            },
        };
        SpiritNexusWork::SemaCompleted(output)
    }

    async fn run_effect(&mut self, effect: Self::Effect) -> Self::Work {
        match effect {}
    }

    fn budget_exhausted_reply(&self, _exhausted: ContinuationExhausted) -> Self::Reply {
        Self::unimplemented_reply_for_engine_failure()
    }
}

impl Actor for DispatchPhase {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(
            arguments.classifier,
            arguments.clock,
            arguments.store,
            arguments.state,
            arguments.subscription,
            arguments.reply,
        ))
    }
}

impl Message<RouteRequest> for DispatchPhase {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: RouteRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.route(message.request, message.trace).await
    }
}

impl Message<RouteFrameRequest> for DispatchPhase {
    type Reply = Result<FramePipelineReply>;

    async fn handle(
        &mut self,
        message: RouteFrameRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.route_frame(message.request, message.trace).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spirit_routes_read_commands_to_sema_read_step() {
        let action = SpiritRuntimeEngines::action_for_command(Command::ReadState);

        assert!(matches!(action, SpiritNexusAction::CommandSemaRead(_)));
    }

    #[test]
    fn spirit_routes_write_commands_to_sema_write_step() {
        let action =
            SpiritRuntimeEngines::action_for_command(Command::AssertEntry(signal_spirit::Entry {
                topics: signal_spirit::Topics::single(signal_spirit::Topic::new("workspace")),
                kind: signal_spirit::Kind::Decision,
                description: signal_spirit::Description::new("runner command"),
                certainty: signal_spirit::Magnitude::High,
                privacy: signal_spirit::Magnitude::Zero,
            }));

        assert!(matches!(action, SpiritNexusAction::CommandSemaWrite(_)));
    }
}
