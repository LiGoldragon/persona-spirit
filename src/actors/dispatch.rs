use std::sync::{Arc, Mutex};

use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use signal_executor::{
    BatchEffects, BatchPlan, CommandEffect, CommandExecutor, Executor, Lowering, ObserverChannel,
    ObserverSet, OperationEffects, OperationPlan,
};
use signal_frame::{NonEmpty, Request};
use signal_persona_spirit::{
    EffectEmitted, Operation as WorkingOperation, OperationKind, OperationReceived,
    RecordObservation, Reply as WorkingReply, RequestUnimplemented, UnimplementedReason,
};

use crate::observation::{Command, Effect};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpiritLowering;

struct SpiritCommandExecutor {
    classifier: ActorRef<classifier::ClassifierPlane>,
    clock: ActorRef<clock::ClockPlane>,
    store: ActorRef<store::RecordStore>,
    state: ActorRef<state::StatePlane>,
    subscription: ActorRef<subscription::SubscriptionPlane>,
    reply: ActorRef<reply::ReplyShaper>,
    trace: SharedTrace,
}

struct SpiritObserverRecorder {
    trace: SharedTrace,
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
        let command_executor = SpiritCommandExecutor::new(
            self.classifier.clone(),
            self.clock.clone(),
            self.store.clone(),
            self.state.clone(),
            self.subscription.clone(),
            self.reply.clone(),
            trace.clone(),
        );
        let observer = SpiritObserverRecorder::new(trace.clone());
        let observers = ObserverSet::new(observer);
        let mut executor = Executor::new(SpiritLowering, command_executor, observers);
        let reply = executor.execute(request).await;
        Ok(FramePipelineReply::new(reply, trace.snapshot()))
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

impl SpiritLowering {
    fn unimplemented_reply(_operation: &WorkingOperation) -> WorkingReply {
        WorkingReply::RequestUnimplemented(RequestUnimplemented {
            reason: UnimplementedReason::IntegrationNotLanded,
        })
    }
}

impl Lowering for SpiritLowering {
    type Operation = WorkingOperation;
    type Reply = WorkingReply;
    type Command = Command;
    type ComponentEffect = Effect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> std::result::Result<OperationPlan<Command>, WorkingReply> {
        Command::from_request(operation.clone())
            .map(OperationPlan::single)
            .ok_or_else(|| Self::unimplemented_reply(operation))
    }

    fn reply_from_effects(
        &self,
        _operation: &Self::Operation,
        effects: &OperationEffects<Command, Effect>,
    ) -> WorkingReply {
        // Spirit currently lowers each operation to a one-command plan.
        // If an operation grows a multi-command pipeline, the final
        // command effect is the canonical reply by convention.
        effects
            .component_effects()
            .last()
            .expect("persona-spirit operation effects are non-empty")
            .clone()
            .into_reply()
    }
}

impl SpiritCommandExecutor {
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
        }
    }

    async fn execute_operation_plan(
        &self,
        plan: OperationPlan<Command>,
    ) -> Result<OperationEffects<Command, Effect>> {
        let mut command_effects = Vec::new();
        for command in plan.into_commands() {
            command_effects.push(self.execute_command(command).await?);
        }
        Ok(OperationEffects::new(
            NonEmpty::try_from_vec(command_effects)
                .expect("operation plans are statically non-empty"),
        ))
    }

    async fn execute_command(&self, command: Command) -> Result<CommandEffect<Command, Effect>> {
        let reply = match command.clone() {
            Command::ClassifyStatement(statement) => self.classify_statement(statement).await?,
            Command::AssertEntry(entry) => self.capture_entry(entry).await?,
            Command::ReadRecords(observation) => self.observe_records(observation).await?,
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
        Ok(CommandEffect::new(command, Effect::from_reply(reply)))
    }

    async fn capture_entry(&self, entry: signal_persona_spirit::Entry) -> Result<WorkingReply> {
        let entry = self.stamp_entry(entry).await?;
        self.capture_stamped_entry(entry).await
    }

    async fn stamp_entry(&self, entry: signal_persona_spirit::Entry) -> Result<StampedEntry> {
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

    async fn classify_statement(
        &self,
        statement: signal_persona_spirit::Statement,
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
        subscription: signal_persona_spirit::RecordSubscription,
    ) -> Result<WorkingReply> {
        let trace = self.trace.snapshot();
        let snapshot = self
            .store
            .ask(store::ReadRecordSnapshot {
                subscription: subscription.clone(),
                trace,
            })
            .await
            .map_err(Self::store_send_error)?;
        self.trace.replace(snapshot.trace.clone());
        let pipeline = self
            .subscription
            .ask(subscription::OpenRecordSubscription {
                subscription,
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
        token: signal_persona_spirit::StateSubscriptionToken,
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
        token: signal_persona_spirit::RecordSubscriptionToken,
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
}

impl CommandExecutor for SpiritCommandExecutor {
    type Command = Command;
    type ComponentEffect = Effect;
    type Error = Error;

    async fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Command, Self::ComponentEffect>> {
        // Degenerate atomicity: today's Spirit lowering emits exactly
        // one command plan per request and exactly one command per plan.
        // Multi-operation batches are rejected before any command runs,
        // so the single committed command is the whole atomic unit.
        let operation_count = plan.operations().len();
        if operation_count != 1 {
            return Err(Error::UnsupportedAtomicBatch { operation_count });
        }
        let operation = plan.into_operations().into_head();
        let effects = self.execute_operation_plan(operation).await?;
        Ok(BatchEffects::single(effects))
    }
}

impl SpiritObserverRecorder {
    fn new(trace: SharedTrace) -> Self {
        Self { trace }
    }
}

impl ObserverChannel<WorkingOperation, CommandEffect<Command, Effect>> for SpiritObserverRecorder {
    fn publish_operation_received(&self, operation: &WorkingOperation) {
        let _event = OperationReceived {
            operation: operation.kind(),
        };
        self.trace
            .record(TraceNode::SIGNAL_EXECUTOR, TraceAction::OperationReceived);
    }

    fn publish_effect_emitted(&self, effect: &CommandEffect<Command, Effect>) {
        let _event = EffectEmitted {
            observation: effect.sema_observation(),
        };
        self.trace
            .record(TraceNode::SEMA_OBSERVER, TraceAction::ObservationProjected);
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
