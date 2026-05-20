use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use signal_persona_spirit::{Observation, SpiritRequest, Subscription, SubscriptionToken};

use crate::{Error, Result};

use super::classifier;
use super::pipeline::PipelineReply;
use super::reply;
use super::state;
use super::store;
use super::subscription;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct DispatchPhase {
    classifier: ActorRef<classifier::ClassifierPlane>,
    store: ActorRef<store::RecordStore>,
    state: ActorRef<state::StatePlane>,
    subscription: ActorRef<subscription::SubscriptionPlane>,
    reply: ActorRef<reply::ReplyShaper>,
}

#[derive(Clone)]
pub struct Arguments {
    pub classifier: ActorRef<classifier::ClassifierPlane>,
    pub store: ActorRef<store::RecordStore>,
    pub state: ActorRef<state::StatePlane>,
    pub subscription: ActorRef<subscription::SubscriptionPlane>,
    pub reply: ActorRef<reply::ReplyShaper>,
}

pub struct RouteRequest {
    pub request: SpiritRequest,
    pub trace: ActorTrace,
}

impl DispatchPhase {
    fn new(
        classifier: ActorRef<classifier::ClassifierPlane>,
        store: ActorRef<store::RecordStore>,
        state: ActorRef<state::StatePlane>,
        subscription: ActorRef<subscription::SubscriptionPlane>,
        reply: ActorRef<reply::ReplyShaper>,
    ) -> Self {
        Self {
            classifier,
            store,
            state,
            subscription,
            reply,
        }
    }

    async fn route(&self, request: SpiritRequest, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::DISPATCH_PHASE, TraceAction::MessageReceived);
        match request {
            SpiritRequest::State(statement) => self.classify_statement(statement, trace).await,
            SpiritRequest::Record(entry) => self.capture_entry(entry, trace).await,
            SpiritRequest::Observe(Observation::Records(query)) => {
                self.observe_records(query, trace).await
            }
            SpiritRequest::Observe(Observation::State(_observation)) => {
                self.observe_state(trace).await
            }
            SpiritRequest::Observe(Observation::Questions(_pending)) => {
                self.observe_questions(trace).await
            }
            SpiritRequest::Watch(Subscription::State(_subscription)) => {
                self.subscribe_state(trace).await
            }
            SpiritRequest::Watch(Subscription::Records(subscription)) => {
                self.subscribe_records(subscription, trace).await
            }
            SpiritRequest::Unwatch(SubscriptionToken::State(token)) => {
                self.retract_state_subscription(token, trace).await
            }
            SpiritRequest::Unwatch(SubscriptionToken::Records(token)) => {
                self.retract_record_subscription(token, trace).await
            }
            other => self
                .reply
                .ask(reply::ShapeUnimplemented {
                    operation: other.operation_kind(),
                    trace,
                })
                .await
                .map_err(|error| Error::actor_runtime(error.to_string())),
        }
    }

    async fn capture_entry(
        &self,
        entry: signal_persona_spirit::Entry,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        self.store
            .ask(store::CaptureEntry { entry, trace })
            .await
            .map_err(Self::pipeline_send_error)
    }

    async fn classify_statement(
        &self,
        statement: signal_persona_spirit::Statement,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        let classified = self
            .classifier
            .ask(classifier::ClassifyStatement { statement, trace })
            .await
            .map_err(Self::classifier_send_error)?;
        self.capture_entry(classified.entry, classified.trace).await
    }

    async fn observe_records(
        &self,
        query: signal_persona_spirit::RecordQuery,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        self.store
            .ask(store::ObserveRecords {
                observation: signal_persona_spirit::RecordObservation { query },
                trace,
            })
            .await
            .map_err(Self::pipeline_send_error)
    }

    async fn observe_state(&self, trace: ActorTrace) -> Result<PipelineReply> {
        self.state
            .ask(state::ObserveState { trace })
            .await
            .map_err(Self::state_send_error)
    }

    async fn observe_questions(&self, trace: ActorTrace) -> Result<PipelineReply> {
        self.state
            .ask(state::ObserveQuestions { trace })
            .await
            .map_err(Self::state_send_error)
    }

    async fn subscribe_state(&self, trace: ActorTrace) -> Result<PipelineReply> {
        let snapshot = self
            .state
            .ask(state::ReadStateSnapshot { trace })
            .await
            .map_err(Self::state_send_error)?;
        self.subscription
            .ask(subscription::OpenStateSubscription {
                snapshot: snapshot.state,
                trace: snapshot.trace,
            })
            .await
            .map_err(Self::subscription_send_error)
    }

    async fn subscribe_records(
        &self,
        subscription: signal_persona_spirit::RecordSubscription,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        let snapshot = self
            .store
            .ask(store::ReadRecordSnapshot {
                subscription: subscription.clone(),
                trace,
            })
            .await
            .map_err(Self::pipeline_send_error)?;
        self.subscription
            .ask(subscription::OpenRecordSubscription {
                subscription,
                snapshot: snapshot.records,
                trace: snapshot.trace,
            })
            .await
            .map_err(Self::subscription_send_error)
    }

    async fn retract_state_subscription(
        &self,
        token: signal_persona_spirit::StateSubscriptionToken,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        self.subscription
            .ask(subscription::RetractStateSubscription { token, trace })
            .await
            .map_err(Self::subscription_send_error)
    }

    async fn retract_record_subscription(
        &self,
        token: signal_persona_spirit::RecordSubscriptionToken,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        self.subscription
            .ask(subscription::RetractRecordSubscription { token, trace })
            .await
            .map_err(Self::subscription_send_error)
    }

    fn pipeline_send_error<Message>(error: SendError<Message, Error>) -> Error {
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

    fn subscription_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
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
