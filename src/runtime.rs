use crate::{
    Error, Result, SingleArgument, SocketPath, SpiritActorRuntime, SpiritSignalClient,
    StoreLocation,
};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_persona_spirit::{
    RequestUnimplemented, SpiritReply, SpiritRequest, UnimplementedReason,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritClient {
    request: SingleArgument,
    target: ClientTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientTarget {
    Daemon(SocketPath),
    OneShot(StoreLocation),
}

impl SpiritClient {
    pub fn from_argument(request: SingleArgument) -> Self {
        Self {
            request,
            target: ClientTarget::from_environment(),
        }
    }

    pub fn with_store(request: SingleArgument, store: StoreLocation) -> Self {
        Self {
            request,
            target: ClientTarget::OneShot(store),
        }
    }

    pub fn with_socket(request: SingleArgument, socket: SocketPath) -> Self {
        Self {
            request,
            target: ClientTarget::Daemon(socket),
        }
    }

    pub fn run(&self) -> Result<()> {
        println!("{}", self.reply_text()?);
        Ok(())
    }

    pub fn reply_text(&self) -> Result<String> {
        self.target.reply_text(self.request.as_str())
    }
}

impl ClientTarget {
    pub fn from_environment() -> Self {
        match std::env::var("PERSONA_SPIRIT_SOCKET") {
            Ok(socket) => Self::Daemon(SocketPath::new(socket)),
            Err(_) => Self::OneShot(StoreLocation::from_environment()),
        }
    }

    fn reply_text(&self, request_text: &str) -> Result<String> {
        match self {
            Self::Daemon(socket) => self.daemon_reply_text(socket, request_text),
            Self::OneShot(store) => {
                SpiritActorRuntime::submit_text_blocking(store.clone(), request_text.to_string())
                    .map(|reply| reply.into_text())
            }
        }
    }

    fn daemon_reply_text(&self, socket: &SocketPath, request_text: &str) -> Result<String> {
        let request = SpiritRequestText::new(request_text).decode_request()?;
        let reply = SpiritSignalClient::new(socket.clone()).submit(request)?;
        SpiritReplyText::new(reply).encode()
    }
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
