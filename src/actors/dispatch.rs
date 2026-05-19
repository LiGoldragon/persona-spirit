use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use signal_persona_spirit::SpiritRequest;

use crate::{Error, Result};

use super::pipeline::PipelineReply;
use super::reply;
use super::state;
use super::store;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct DispatchPhase {
    store: ActorRef<store::RecordStore>,
    state: ActorRef<state::StatePlane>,
    reply: ActorRef<reply::ReplyShaper>,
}

#[derive(Clone)]
pub struct Arguments {
    pub store: ActorRef<store::RecordStore>,
    pub state: ActorRef<state::StatePlane>,
    pub reply: ActorRef<reply::ReplyShaper>,
}

pub struct RouteRequest {
    pub request: SpiritRequest,
    pub trace: ActorTrace,
}

impl DispatchPhase {
    fn new(
        store: ActorRef<store::RecordStore>,
        state: ActorRef<state::StatePlane>,
        reply: ActorRef<reply::ReplyShaper>,
    ) -> Self {
        Self {
            store,
            state,
            reply,
        }
    }

    async fn route(&self, request: SpiritRequest, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::DISPATCH_PHASE, TraceAction::MessageReceived);
        match request {
            SpiritRequest::Entry(entry) => self.capture_entry(entry, trace).await,
            SpiritRequest::RecordObservation(observation) => {
                self.observe_records(observation, trace).await
            }
            SpiritRequest::StateObservation(_observation) => self.observe_state(trace).await,
            SpiritRequest::QuestionPending(_pending) => self.observe_questions(trace).await,
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

    async fn observe_records(
        &self,
        observation: signal_persona_spirit::RecordObservation,
        trace: ActorTrace,
    ) -> Result<PipelineReply> {
        self.store
            .ask(store::ObserveRecords { observation, trace })
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

    fn pipeline_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn state_send_error<Message>(error: SendError<Message, Infallible>) -> Error {
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
        Ok(Self::new(arguments.store, arguments.state, arguments.reply))
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
