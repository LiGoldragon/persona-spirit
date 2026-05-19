use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

use nota_codec::{Decoder, NotaDecode, NotaTransparent};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    SignalVerb, SubReply,
};
use signal_persona_spirit::{Frame, FrameBody, SpiritReply, SpiritRequest};

use crate::{Error, Result, StoreLocation, actors::root::SpiritRoot};

const DEFAULT_MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, nota_codec::NotaRecord)]
pub struct DaemonConfiguration {
    pub socket_path: SocketPath,
    pub store_path: StorePath,
    pub socket_mode: SocketMode,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaTransparent)]
pub struct SocketPath(String);

#[derive(Debug, Clone, PartialEq, Eq, NotaTransparent)]
pub struct StorePath(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, NotaTransparent)]
pub struct SocketMode(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpiritFrameCodec {
    maximum_frame_bytes: usize,
}

pub struct DaemonRuntime {
    configuration: DaemonConfiguration,
}

pub struct BoundDaemon {
    socket: SocketPath,
    listener: UnixListener,
    runtime: tokio::runtime::Runtime,
    root: kameo::actor::ActorRef<SpiritRoot>,
    codec: SpiritFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritSignalClient {
    socket: SocketPath,
    codec: SpiritFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRequest {
    exchange: ExchangeIdentifier,
    request: signal_core::Request<SpiritRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedExchange {
    reply: Reply<SpiritReply>,
}

impl DaemonConfiguration {
    pub fn new(socket_path: SocketPath, store_path: StorePath, socket_mode: SocketMode) -> Self {
        Self {
            socket_path,
            store_path,
            socket_mode,
        }
    }

    pub fn from_text(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let configuration =
            Self::decode(&mut decoder).map_err(Error::invalid_daemon_configuration)?;
        StrictEnd::new(&mut decoder).expect()?;
        Ok(configuration)
    }

    pub fn store_location(&self) -> StoreLocation {
        StoreLocation::new(self.store_path.as_path())
    }
}

impl SocketPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl StorePath {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

impl Default for SpiritFrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAXIMUM_FRAME_BYTES)
    }
}

impl SpiritFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, stream: &mut UnixStream) -> Result<Frame> {
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .map_err(Error::input_output)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::FrameTooLarge {
                found: length,
                limit: self.maximum_frame_bytes,
            });
        }

        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream
            .read_exact(&mut bytes[4..])
            .map_err(Error::input_output)?;
        Frame::decode_length_prefixed(&bytes).map_err(Error::signal_frame)
    }

    pub fn write_frame(&self, stream: &mut UnixStream, frame: &Frame) -> Result<()> {
        let bytes = frame
            .encode_length_prefixed()
            .map_err(Error::signal_frame)?;
        stream.write_all(&bytes).map_err(Error::input_output)?;
        stream.flush().map_err(Error::input_output)
    }

    pub fn request_frame(&self, request: SpiritRequest) -> Frame {
        Frame::new(FrameBody::Request {
            exchange: self.exchange(),
            request: request.into_request(),
        })
    }

    pub fn reply_frame(&self, exchange: ExchangeIdentifier, reply: Reply<SpiritReply>) -> Frame {
        Frame::new(FrameBody::Reply { exchange, reply })
    }

    pub fn request_from_frame(&self, frame: Frame) -> Result<ReceivedRequest> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => Ok(ReceivedRequest { exchange, request }),
            other => Err(Error::UnexpectedFrame {
                expected: "request",
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn reply_from_frame(&self, frame: Frame) -> Result<Reply<SpiritReply>> {
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => Ok(reply),
            other => Err(Error::UnexpectedFrame {
                expected: "reply",
                got: format!("{other:?}"),
            }),
        }
    }

    fn exchange(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }
}

impl DaemonRuntime {
    pub fn from_configuration(configuration: DaemonConfiguration) -> Self {
        Self { configuration }
    }

    pub fn from_argument(argument: crate::SingleArgument) -> Result<Self> {
        Ok(Self::from_configuration(DaemonConfiguration::from_text(
            argument.as_str(),
        )?))
    }

    pub fn run(self) -> Result<()> {
        self.bind()?.serve_forever()
    }

    pub fn bind(self) -> Result<BoundDaemon> {
        SocketBinding::bind(
            &self.configuration.socket_path,
            self.configuration.socket_mode,
        )?;
        let listener = UnixListener::bind(self.configuration.socket_path.as_path())
            .map_err(Error::input_output)?;
        std::fs::set_permissions(
            self.configuration.socket_path.as_path(),
            std::fs::Permissions::from_mode(self.configuration.socket_mode.as_octal()),
        )
        .map_err(Error::input_output)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        let root = runtime.block_on(SpiritRoot::start(crate::actors::root::Arguments::new(
            self.configuration.store_location(),
        )))?;
        Ok(BoundDaemon {
            socket: self.configuration.socket_path,
            listener,
            runtime,
            root,
            codec: SpiritFrameCodec::default(),
        })
    }
}

impl BoundDaemon {
    pub fn socket_path(&self) -> &Path {
        self.socket.as_path()
    }

    pub fn serve_one(&mut self) -> Result<ServedExchange> {
        let (mut stream, _address) = self.listener.accept().map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = self.reply_to_request(received.request)?;
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedExchange::new(reply))
    }

    pub fn serve_count(mut self, count: usize) -> Result<Vec<ServedExchange>> {
        let result = (0..count)
            .map(|_| self.serve_one())
            .collect::<Result<Vec<_>>>();
        let shutdown = self.shutdown();
        match (result, shutdown) {
            (Ok(served), Ok(())) => Ok(served),
            (Err(error), _) => Err(error),
            (Ok(_served), Err(error)) => Err(error),
        }
    }

    pub fn serve_forever(mut self) -> Result<()> {
        loop {
            if let Err(error) = self.serve_one() {
                eprintln!("persona-spirit-daemon client error: {error}");
            }
        }
    }

    pub fn shutdown(self) -> Result<()> {
        self.runtime
            .block_on(SpiritRoot::stop(self.root))
            .and_then(|()| SocketBinding::remove(&self.socket))
    }

    fn reply_to_request(
        &self,
        request: signal_core::Request<SpiritRequest>,
    ) -> Result<Reply<SpiritReply>> {
        match request.into_checked() {
            Ok(checked) => {
                let replies = checked
                    .operations
                    .into_iter()
                    .map(|operation| self.reply_to_operation(operation.verb, operation.payload))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Reply::completed(
                    NonEmpty::try_from_vec(replies).expect("checked request is non-empty"),
                ))
            }
            Err((reason, _request)) => Ok(Reply::rejected(reason)),
        }
    }

    fn reply_to_operation(
        &self,
        verb: SignalVerb,
        request: SpiritRequest,
    ) -> Result<SubReply<SpiritReply>> {
        let reply = self
            .runtime
            .block_on(async {
                self.root
                    .ask(crate::actors::root::SubmitRequest { request })
                    .await
            })
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        Ok(SubReply::Ok {
            verb,
            payload: reply.into_reply(),
        })
    }
}

impl SpiritSignalClient {
    pub fn new(socket: SocketPath) -> Self {
        Self {
            socket,
            codec: SpiritFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: SpiritRequest) -> Result<SpiritReply> {
        let mut stream = UnixStream::connect(self.socket.as_path()).map_err(Error::input_output)?;
        let frame = self.codec.request_frame(request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.reply_payload(self.codec.reply_from_frame(reply)?)
    }

    fn reply_payload(&self, reply: Reply<SpiritReply>) -> Result<SpiritReply> {
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok { payload, .. } => Ok(payload),
                other => Err(Error::UnexpectedFrame {
                    expected: "accepted operation reply",
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::RequestRejected { reason }),
        }
    }
}

impl ServedExchange {
    fn new(reply: Reply<SpiritReply>) -> Self {
        Self { reply }
    }

    pub fn reply(&self) -> &Reply<SpiritReply> {
        &self.reply
    }
}

struct SocketBinding;

impl SocketBinding {
    fn bind(socket: &SocketPath, _mode: SocketMode) -> Result<()> {
        if let Some(parent) = socket.as_path().parent() {
            std::fs::create_dir_all(parent).map_err(Error::input_output)?;
        }
        Self::remove(socket)
    }

    fn remove(socket: &SocketPath) -> Result<()> {
        match std::fs::remove_file(socket.as_path()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::input_output(error)),
        }
    }
}

struct StrictEnd<'decoder, 'input> {
    decoder: &'decoder mut Decoder<'input>,
}

impl<'decoder, 'input> StrictEnd<'decoder, 'input> {
    fn new(decoder: &'decoder mut Decoder<'input>) -> Self {
        Self { decoder }
    }

    fn expect(&mut self) -> Result<()> {
        if let Some(token) = self
            .decoder
            .peek_token()
            .map_err(Error::invalid_daemon_configuration)?
        {
            Err(Error::InvalidDaemonConfiguration {
                reason: format!("expected end of input, got {token:?}"),
            })
        } else {
            Ok(())
        }
    }
}
