use crate::{Error, Result, SingleArgument, SocketPath, SpiritSignalClient};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_persona_spirit::{SpiritReply, SpiritRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritClient {
    request: SingleArgument,
    socket: SocketPath,
}

impl SpiritClient {
    pub fn from_argument(request: SingleArgument) -> Result<Self> {
        Ok(Self {
            request,
            socket: SocketPath::from_environment()?,
        })
    }

    pub fn with_socket(request: SingleArgument, socket: SocketPath) -> Self {
        Self { request, socket }
    }

    pub fn run(&self) -> Result<()> {
        println!("{}", self.reply_text()?);
        Ok(())
    }

    pub fn reply_text(&self) -> Result<String> {
        self.daemon_reply_text(self.request.as_str())
    }

    fn daemon_reply_text(&self, request_text: &str) -> Result<String> {
        let request = SpiritRequestText::new(request_text).decode_request()?;
        let reply = SpiritSignalClient::new(self.socket.clone()).submit(request)?;
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
