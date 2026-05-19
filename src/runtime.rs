use crate::{Error, Result, SingleArgument, SpiritActorRuntime, StoreLocation};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_persona_spirit::{
    RequestUnimplemented, SpiritReply, SpiritRequest, UnimplementedReason,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritClient {
    request: SingleArgument,
    store: StoreLocation,
}

impl SpiritClient {
    pub fn from_argument(request: SingleArgument) -> Self {
        Self {
            request,
            store: StoreLocation::from_environment(),
        }
    }

    pub fn with_store(request: SingleArgument, store: StoreLocation) -> Self {
        Self { request, store }
    }

    pub fn run(&self) -> Result<()> {
        println!("{}", self.reply_text()?);
        Ok(())
    }

    pub fn reply_text(&self) -> Result<String> {
        SpiritActorRuntime::submit_text_blocking(self.store.clone(), self.request.as_str())
            .map(|reply| reply.into_text())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonRuntime {
    configuration: SingleArgument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritRequestText {
    text: String,
}

impl SpiritRequestText {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn reply_text(&self) -> Result<String> {
        let request = self.decode_request()?;
        SpiritReplyText::new(SpiritReply::RequestUnimplemented(RequestUnimplemented {
            operation: request.operation_kind(),
            reason: UnimplementedReason::NotBuiltYet,
        }))
        .encode()
    }

    pub fn decode_request(&self) -> Result<SpiritRequest> {
        let mut decoder = Decoder::new(&self.text);
        let request = SpiritRequest::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        SpiritRequestEnd::new(&mut decoder).expect()?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritReplyText {
    reply: SpiritReply,
}

impl SpiritReplyText {
    pub fn new(reply: SpiritReply) -> Self {
        Self { reply }
    }

    pub fn encode(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.reply
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        Ok(encoder.into_string())
    }
}

struct SpiritRequestEnd<'decoder, 'input> {
    decoder: &'decoder mut Decoder<'input>,
}

impl<'decoder, 'input> SpiritRequestEnd<'decoder, 'input> {
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

impl DaemonRuntime {
    pub fn from_argument(configuration: SingleArgument) -> Self {
        Self { configuration }
    }

    pub fn run(&self) -> Result<()> {
        let _configuration_text = self.configuration.as_str();
        Err(Error::RuntimeNotImplemented {
            surface: "persona-spirit-daemon",
            reason: "Kameo actor tree, sema-engine state, and sockets are not implemented",
        })
    }
}
