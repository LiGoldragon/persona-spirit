use std::fs;

use crate::{
    Error, Result, SingleArgument, SocketPath,
    daemon::{OwnerSignalClient, SignalClient},
};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use owner_signal_persona_spirit::{Operation as OwnerOperation, Reply as OwnerReply};
use signal_frame::CommandLineSocket;
use signal_persona_spirit::{Operation as WorkingOperation, Reply as WorkingReply};

signal_frame::signal_cli! {
    pub struct CommandLineDispatch {
        working signal_persona_spirit::Operation;
        owner owner_signal_persona_spirit::Operation;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Client {
    input: RequestInput,
    sockets: CommandLineSockets,
}

impl Client {
    pub fn from_argument(argument: SingleArgument) -> Result<Self> {
        Ok(Self {
            input: RequestInput::new(argument),
            sockets: CommandLineSockets::from_environment(),
        })
    }

    pub fn with_socket(argument: SingleArgument, socket: SocketPath) -> Self {
        Self {
            input: RequestInput::new(argument),
            sockets: CommandLineSockets::ordinary_only(socket),
        }
    }

    pub fn with_sockets(
        argument: SingleArgument,
        ordinary_socket: SocketPath,
        owner_socket: SocketPath,
    ) -> Self {
        Self {
            input: RequestInput::new(argument),
            sockets: CommandLineSockets::new(Some(ordinary_socket), Some(owner_socket)),
        }
    }

    pub fn run(&self) -> Result<()> {
        println!("{}", self.reply_text()?);
        Ok(())
    }

    pub fn reply_text(&self) -> Result<String> {
        self.daemon_reply_text(&self.input.text()?)
    }

    fn daemon_reply_text(&self, request_text: &str) -> Result<String> {
        match RequestHead::from_text(request_text)?.route()? {
            CommandLineSocket::Working => {
                let request = RequestText::new(request_text).decode_request()?;
                let reply =
                    SignalClient::new(self.sockets.ordinary_socket()?.clone()).submit(request)?;
                ReplyText::new(reply).encode()
            }
            CommandLineSocket::Owner => {
                let request = OwnerRequestText::new(request_text).decode_request()?;
                let reply =
                    OwnerSignalClient::new(self.sockets.owner_socket()?.clone()).submit(request)?;
                OwnerReplyText::new(reply).encode()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLineSockets {
    ordinary_socket: Option<SocketPath>,
    owner_socket: Option<SocketPath>,
}

impl CommandLineSockets {
    pub fn new(ordinary_socket: Option<SocketPath>, owner_socket: Option<SocketPath>) -> Self {
        Self {
            ordinary_socket,
            owner_socket,
        }
    }

    pub fn from_environment() -> Self {
        Self {
            ordinary_socket: std::env::var("PERSONA_SPIRIT_SOCKET")
                .ok()
                .map(SocketPath::new),
            owner_socket: std::env::var("PERSONA_SPIRIT_OWNER_SOCKET")
                .ok()
                .map(SocketPath::new),
        }
    }

    pub fn ordinary_only(socket: SocketPath) -> Self {
        Self::new(Some(socket), None)
    }

    pub fn ordinary_socket(&self) -> Result<&SocketPath> {
        self.ordinary_socket
            .as_ref()
            .ok_or(Error::MissingSpiritSocket)
    }

    pub fn owner_socket(&self) -> Result<&SocketPath> {
        self.owner_socket
            .as_ref()
            .ok_or(Error::MissingOwnerSpiritSocket)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestInput {
    argument: SingleArgument,
}

impl RequestInput {
    pub fn new(argument: SingleArgument) -> Self {
        Self { argument }
    }

    pub fn text(&self) -> Result<String> {
        let value = self.argument.as_str();
        if value.starts_with('(') {
            Ok(value.to_string())
        } else {
            fs::read_to_string(value).map_err(Error::input_output)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    head: String,
}

impl RequestHead {
    pub fn from_text(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let head = decoder
            .peek_record_head()
            .map_err(Error::invalid_spirit_request)?;
        Ok(Self { head })
    }

    pub fn as_str(&self) -> &str {
        &self.head
    }

    pub fn route(&self) -> Result<CommandLineSocket> {
        CommandLineDispatch::new()
            .route_head(self.as_str())
            .map_err(Error::command_line_route)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestText {
    text: String,
}

impl RequestText {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn decode_request(&self) -> Result<WorkingOperation> {
        let mut decoder = Decoder::new(&self.text);
        let request =
            WorkingOperation::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        RequestEnd::new(&mut decoder).expect()?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRequestText {
    text: String,
}

impl OwnerRequestText {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn decode_request(&self) -> Result<OwnerOperation> {
        let mut decoder = Decoder::new(&self.text);
        let request =
            OwnerOperation::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        RequestEnd::new(&mut decoder).expect()?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyText {
    reply: WorkingReply,
}

impl ReplyText {
    pub fn new(reply: WorkingReply) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReplyText {
    reply: OwnerReply,
}

impl OwnerReplyText {
    pub fn new(reply: OwnerReply) -> Self {
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
