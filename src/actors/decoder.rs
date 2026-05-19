use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nota_codec::{Decoder, NotaDecode};
use signal_persona_spirit::SpiritRequest;

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

        let mut decoder = Decoder::new(text);
        let request = SpiritRequest::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        if self.strict_end {
            RequestEnd::new(&mut decoder).expect()?;
        }

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

struct RequestEnd<'decoder, 'input> {
    decoder: &'decoder mut Decoder<'input>,
}

impl<'decoder, 'input> RequestEnd<'decoder, 'input> {
    fn new(decoder: &'decoder mut Decoder<'input>) -> Self {
        Self { decoder }
    }

    fn expect(&mut self) -> Result<()> {
        if let Some(token) = self
            .decoder
            .peek_token()
            .map_err(Error::invalid_spirit_request)?
        {
            Err(Error::InvalidSpiritRequest {
                reason: format!("expected end of input, got {token:?}"),
            })
        } else {
            Ok(())
        }
    }
}
