use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};

use crate::{Error, Result};

use super::decoder;
use super::dispatch;
use super::pipeline::{DecodedRequest, PipelineReply};
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct IngressPhase {
    decoder: ActorRef<decoder::NotaDecoder>,
    dispatch: ActorRef<dispatch::DispatchPhase>,
}

#[derive(Clone)]
pub struct Arguments {
    pub decoder: ActorRef<decoder::NotaDecoder>,
    pub dispatch: ActorRef<dispatch::DispatchPhase>,
}

pub struct AcceptText {
    pub text: String,
    pub trace: ActorTrace,
}

impl IngressPhase {
    fn new(
        decoder: ActorRef<decoder::NotaDecoder>,
        dispatch: ActorRef<dispatch::DispatchPhase>,
    ) -> Self {
        Self { decoder, dispatch }
    }

    async fn accept(&self, text: String, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::INGRESS_PHASE, TraceAction::MessageReceived);
        let decoded = self.decode(text, trace).await?;
        let (request, trace) = decoded.into_parts();
        self.dispatch
            .ask(dispatch::RouteRequest { request, trace })
            .await
            .map_err(Self::pipeline_send_error)
    }

    async fn decode(&self, text: String, trace: ActorTrace) -> Result<DecodedRequest> {
        self.decoder
            .ask(decoder::DecodeText { text, trace })
            .await
            .map_err(Self::decoded_send_error)
    }

    fn pipeline_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn decoded_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }
}

impl Actor for IngressPhase {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.decoder, arguments.dispatch))
    }
}

impl Message<AcceptText> for IngressPhase {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: AcceptText,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.accept(message.text, message.trace).await
    }
}
