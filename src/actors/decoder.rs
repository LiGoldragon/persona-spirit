use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nota_next::NotaSource;
use signal_spirit::Operation as WorkingOperation;

use crate::{Error, Result};

use super::pipeline::DecodedRequest;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct NotaDecoder {
    strict_end: bool,
}

#[derive(Clone)]
pub struct Arguments {
    pub strict_end: bool,
}

pub struct DecodeText {
    pub text: String,
    pub trace: ActorTrace,
}

impl Default for Arguments {
    fn default() -> Self {
        Self { strict_end: true }
    }
}

impl NotaDecoder {
    fn new(strict_end: bool) -> Self {
        Self { strict_end }
    }

    fn decode_text(&self, text: &str, mut trace: ActorTrace) -> Result<DecodedRequest> {
        trace.record(TraceNode::NOTA_DECODER, TraceAction::MessageReceived);

        let request = NotaSource::new(text)
            .parse::<WorkingOperation>()
            .map_err(Error::invalid_spirit_request)?;
        let _strict_end = self.strict_end;

        trace.record(TraceNode::NOTA_DECODER, TraceAction::RequestDecoded);
        Ok(DecodedRequest::new(request, trace))
    }
}

impl Actor for NotaDecoder {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.strict_end))
    }
}

impl Message<DecodeText> for NotaDecoder {
    type Reply = Result<DecodedRequest>;

    async fn handle(
        &mut self,
        message: DecodeText,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.decode_text(&message.text, message.trace)
    }
}
