use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use nota_codec::{Decoder, NotaDecode, NotaTransparent};
use owner_signal_persona_spirit::{
    Frame as OwnerFrame, FrameBody as OwnerFrameBody, OwnerSpiritReply, OwnerSpiritRequest,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    SubReply,
};
use signal_persona_spirit::{Frame, FrameBody, SpiritReply, SpiritRequest};

use crate::{
    Error, Result, StoreLocation,
    actors::{policy::BootstrapPolicySource, root::SpiritRoot},
};

const DEFAULT_MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, nota_codec::NotaRecord)]
pub struct DaemonConfiguration {
    pub ordinary_socket_path: SocketPath,
    pub owner_socket_path: SocketPath,
    pub store_path: StorePath,
    pub socket_mode: SocketMode,
    pub bootstrap_policy_path: Option<BootstrapPolicyPath>,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaTransparent)]
pub struct SocketPath(String);

#[derive(Debug, Clone, PartialEq, Eq, NotaTransparent)]
pub struct StorePath(String);

#[derive(Debug, Clone, PartialEq, Eq, NotaTransparent)]
pub struct BootstrapPolicyPath(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, NotaTransparent)]
pub struct SocketMode(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpiritFrameCodec {
    maximum_frame_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerSpiritFrameCodec {
    maximum_frame_bytes: usize,
}

pub struct DaemonRuntime {
    configuration: DaemonConfiguration,
}

pub struct BoundDaemon {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    ordinary_listener: UnixListener,
    owner_listener: UnixListener,
    runtime: Arc<tokio::runtime::Runtime>,
    root: kameo::actor::ActorRef<SpiritRoot>,
    codec: SpiritFrameCodec,
    owner_codec: OwnerSpiritFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritSignalClient {
    socket: SocketPath,
    codec: SpiritFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerSpiritSignalClient {
    socket: SocketPath,
    codec: OwnerSpiritFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRequest {
    exchange: ExchangeIdentifier,
    request: signal_frame::Request<SpiritRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedOwnerRequest {
    exchange: ExchangeIdentifier,
    request: signal_frame::Request<OwnerSpiritRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedExchange {
    reply: Reply<SpiritReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedOwnerExchange {
    reply: Reply<OwnerSpiritReply>,
}

impl DaemonConfiguration {
    pub fn new(
        ordinary_socket_path: SocketPath,
        owner_socket_path: SocketPath,
        store_path: StorePath,
        socket_mode: SocketMode,
    ) -> Self {
        Self {
            ordinary_socket_path,
            owner_socket_path,
            store_path,
            socket_mode,
            bootstrap_policy_path: None,
        }
    }

    pub fn with_bootstrap_policy_path(
        mut self,
        bootstrap_policy_path: BootstrapPolicyPath,
    ) -> Self {
        self.bootstrap_policy_path = Some(bootstrap_policy_path);
        self
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

    pub fn bootstrap_policy_source(&self) -> BootstrapPolicySource {
        match &self.bootstrap_policy_path {
            Some(path) => BootstrapPolicySource::path(path.as_path()),
            None => BootstrapPolicySource::default(),
        }
    }
}

impl SocketPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn from_environment() -> Result<Self> {
        std::env::var("PERSONA_SPIRIT_SOCKET")
            .map(Self::new)
            .map_err(|_| Error::MissingSpiritSocket)
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

impl BootstrapPolicyPath {
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

impl Default for OwnerSpiritFrameCodec {
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

impl OwnerSpiritFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, stream: &mut UnixStream) -> Result<OwnerFrame> {
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
        OwnerFrame::decode_length_prefixed(&bytes).map_err(Error::signal_frame)
    }

    pub fn write_frame(&self, stream: &mut UnixStream, frame: &OwnerFrame) -> Result<()> {
        let bytes = frame
            .encode_length_prefixed()
            .map_err(Error::signal_frame)?;
        stream.write_all(&bytes).map_err(Error::input_output)?;
        stream.flush().map_err(Error::input_output)
    }

    pub fn request_frame(&self, request: OwnerSpiritRequest) -> OwnerFrame {
        OwnerFrame::new(OwnerFrameBody::Request {
            exchange: self.exchange(),
            request: request.into_request(),
        })
    }

    pub fn reply_frame(
        &self,
        exchange: ExchangeIdentifier,
        reply: Reply<OwnerSpiritReply>,
    ) -> OwnerFrame {
        OwnerFrame::new(OwnerFrameBody::Reply { exchange, reply })
    }

    pub fn request_from_frame(&self, frame: OwnerFrame) -> Result<ReceivedOwnerRequest> {
        match frame.into_body() {
            OwnerFrameBody::Request { exchange, request } => {
                Ok(ReceivedOwnerRequest { exchange, request })
            }
            other => Err(Error::UnexpectedFrame {
                expected: "owner request",
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn reply_from_frame(&self, frame: OwnerFrame) -> Result<Reply<OwnerSpiritReply>> {
        match frame.into_body() {
            OwnerFrameBody::Reply { reply, .. } => Ok(reply),
            other => Err(Error::UnexpectedFrame {
                expected: "owner reply",
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
            &self.configuration.ordinary_socket_path,
            self.configuration.socket_mode,
        )?;
        SocketBinding::bind(
            &self.configuration.owner_socket_path,
            self.configuration.socket_mode,
        )?;
        let ordinary_listener =
            UnixListener::bind(self.configuration.ordinary_socket_path.as_path())
                .map_err(Error::input_output)?;
        let owner_listener = UnixListener::bind(self.configuration.owner_socket_path.as_path())
            .map_err(Error::input_output)?;
        std::fs::set_permissions(
            self.configuration.ordinary_socket_path.as_path(),
            std::fs::Permissions::from_mode(self.configuration.socket_mode.as_octal()),
        )
        .map_err(Error::input_output)?;
        std::fs::set_permissions(
            self.configuration.owner_socket_path.as_path(),
            std::fs::Permissions::from_mode(self.configuration.socket_mode.as_octal()),
        )
        .map_err(Error::input_output)?;
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|error| Error::actor_runtime(error.to_string()))?,
        );
        let root = runtime.block_on(SpiritRoot::start(
            crate::actors::root::Arguments::with_bootstrap_policy_source(
                self.configuration.store_location(),
                self.configuration.bootstrap_policy_source(),
            ),
        ))?;
        Ok(BoundDaemon {
            ordinary_socket: self.configuration.ordinary_socket_path,
            owner_socket: self.configuration.owner_socket_path,
            ordinary_listener,
            owner_listener,
            runtime,
            root,
            codec: SpiritFrameCodec::default(),
            owner_codec: OwnerSpiritFrameCodec::default(),
        })
    }
}

impl BoundDaemon {
    pub fn socket_path(&self) -> &Path {
        self.ordinary_socket.as_path()
    }

    pub fn ordinary_socket_path(&self) -> &Path {
        self.ordinary_socket.as_path()
    }

    pub fn owner_socket_path(&self) -> &Path {
        self.owner_socket.as_path()
    }

    pub fn serve_one(&mut self) -> Result<ServedExchange> {
        let (mut stream, _address) = self
            .ordinary_listener
            .accept()
            .map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = self.reply_to_request(received.request)?;
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedExchange::new(reply))
    }

    pub fn serve_owner_one(&mut self) -> Result<ServedOwnerExchange> {
        let (mut stream, _address) = self.owner_listener.accept().map_err(Error::input_output)?;
        let frame = self.owner_codec.read_frame(&mut stream)?;
        let received = self.owner_codec.request_from_frame(frame)?;
        let reply = self.reply_to_owner_request(received.request)?;
        let frame = self
            .owner_codec
            .reply_frame(received.exchange, reply.clone());
        self.owner_codec.write_frame(&mut stream, &frame)?;
        Ok(ServedOwnerExchange::new(reply))
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

    pub fn serve_owner_count(mut self, count: usize) -> Result<Vec<ServedOwnerExchange>> {
        let result = (0..count)
            .map(|_| self.serve_owner_one())
            .collect::<Result<Vec<_>>>();
        let shutdown = self.shutdown();
        match (result, shutdown) {
            (Ok(served), Ok(())) => Ok(served),
            (Err(error), _) => Err(error),
            (Ok(_served), Err(error)) => Err(error),
        }
    }

    pub fn serve_forever(self) -> Result<()> {
        let ordinary = SocketServer::new(
            self.ordinary_listener
                .try_clone()
                .map_err(Error::input_output)?,
            self.root.clone(),
            self.runtime.clone(),
            self.codec,
        );
        let owner = OwnerSocketServer::new(
            self.owner_listener
                .try_clone()
                .map_err(Error::input_output)?,
            self.root.clone(),
            self.runtime.clone(),
            self.owner_codec,
        );
        let ordinary_handle = thread::spawn(move || ordinary.serve_forever());
        let owner_result = owner.serve_forever();
        let ordinary_result = ordinary_handle
            .join()
            .map_err(|_| Error::actor_runtime("ordinary socket thread panicked"))?;
        owner_result.and(ordinary_result)
    }

    pub fn shutdown(self) -> Result<()> {
        let stop = self.runtime.block_on(SpiritRoot::stop(self.root));
        let remove_ordinary = SocketBinding::remove(&self.ordinary_socket);
        let remove_owner = SocketBinding::remove(&self.owner_socket);
        match (stop, remove_ordinary, remove_owner) {
            (Ok(()), Ok(()), Ok(())) => Ok(()),
            (Err(error), _, _) => Err(error),
            (Ok(()), Err(error), _) => Err(error),
            (Ok(()), Ok(()), Err(error)) => Err(error),
        }
    }

    fn reply_to_request(
        &self,
        request: signal_frame::Request<SpiritRequest>,
    ) -> Result<Reply<SpiritReply>> {
        OrdinaryExchangeHandler::new(self.root.clone(), self.runtime.clone())
            .reply_to_request(request)
    }

    fn reply_to_owner_request(
        &self,
        request: signal_frame::Request<OwnerSpiritRequest>,
    ) -> Result<Reply<OwnerSpiritReply>> {
        OwnerExchangeHandler::new(self.root.clone(), self.runtime.clone()).reply_to_request(request)
    }
}

struct SocketServer {
    listener: UnixListener,
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    codec: SpiritFrameCodec,
}

struct OwnerSocketServer {
    listener: UnixListener,
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    codec: OwnerSpiritFrameCodec,
}

struct OrdinaryExchangeHandler {
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
}

struct OwnerExchangeHandler {
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SocketServer {
    fn new(
        listener: UnixListener,
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
        codec: SpiritFrameCodec,
    ) -> Self {
        Self {
            listener,
            root,
            runtime,
            codec,
        }
    }

    fn serve_forever(self) -> Result<()> {
        loop {
            if let Err(error) = self.serve_one() {
                eprintln!("persona-spirit-daemon ordinary client error: {error}");
            }
        }
    }

    fn serve_one(&self) -> Result<ServedExchange> {
        let (mut stream, _address) = self.listener.accept().map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = OrdinaryExchangeHandler::new(self.root.clone(), self.runtime.clone())
            .reply_to_request(received.request)?;
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedExchange::new(reply))
    }
}

impl OwnerSocketServer {
    fn new(
        listener: UnixListener,
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
        codec: OwnerSpiritFrameCodec,
    ) -> Self {
        Self {
            listener,
            root,
            runtime,
            codec,
        }
    }

    fn serve_forever(self) -> Result<()> {
        loop {
            if let Err(error) = self.serve_one() {
                eprintln!("persona-spirit-daemon owner client error: {error}");
            }
        }
    }

    fn serve_one(&self) -> Result<ServedOwnerExchange> {
        let (mut stream, _address) = self.listener.accept().map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = OwnerExchangeHandler::new(self.root.clone(), self.runtime.clone())
            .reply_to_request(received.request)?;
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedOwnerExchange::new(reply))
    }
}

impl OrdinaryExchangeHandler {
    fn new(
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self { root, runtime }
    }

    fn reply_to_request(
        &self,
        request: signal_frame::Request<SpiritRequest>,
    ) -> Result<Reply<SpiritReply>> {
        let replies = request
            .payloads
            .into_iter()
            .map(|request| self.reply_to_operation(request))
            .collect::<Result<Vec<_>>>()?;
        Ok(Reply::committed(
            NonEmpty::try_from_vec(replies).expect("request is non-empty"),
        ))
    }

    fn reply_to_operation(&self, request: SpiritRequest) -> Result<SubReply<SpiritReply>> {
        let reply = self
            .runtime
            .block_on(async {
                self.root
                    .ask(crate::actors::root::SubmitRequest { request })
                    .await
            })
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        Ok(SubReply::Ok(reply.into_reply()))
    }
}

impl OwnerExchangeHandler {
    fn new(
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self { root, runtime }
    }

    fn reply_to_request(
        &self,
        request: signal_frame::Request<OwnerSpiritRequest>,
    ) -> Result<Reply<OwnerSpiritReply>> {
        let replies = request
            .payloads
            .into_iter()
            .map(|request| self.reply_to_operation(request))
            .collect::<Result<Vec<_>>>()?;
        Ok(Reply::committed(
            NonEmpty::try_from_vec(replies).expect("request is non-empty"),
        ))
    }

    fn reply_to_operation(
        &self,
        request: OwnerSpiritRequest,
    ) -> Result<SubReply<OwnerSpiritReply>> {
        let reply = self
            .runtime
            .block_on(async {
                self.root
                    .ask(crate::actors::root::SubmitOwnerRequest { request })
                    .await
            })
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        Ok(SubReply::Ok(reply.into_reply()))
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
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::UnexpectedFrame {
                    expected: "accepted operation reply",
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::RequestRejected {
                reason: reason.to_string(),
            }),
        }
    }
}

impl OwnerSpiritSignalClient {
    pub fn new(socket: SocketPath) -> Self {
        Self {
            socket,
            codec: OwnerSpiritFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: OwnerSpiritRequest) -> Result<OwnerSpiritReply> {
        let mut stream = UnixStream::connect(self.socket.as_path()).map_err(Error::input_output)?;
        let frame = self.codec.request_frame(request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.reply_payload(self.codec.reply_from_frame(reply)?)
    }

    fn reply_payload(&self, reply: Reply<OwnerSpiritReply>) -> Result<OwnerSpiritReply> {
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::UnexpectedFrame {
                    expected: "accepted owner operation reply",
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::RequestRejected {
                reason: reason.to_string(),
            }),
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

impl ServedOwnerExchange {
    fn new(reply: Reply<OwnerSpiritReply>) -> Self {
        Self { reply }
    }

    pub fn reply(&self) -> &Reply<OwnerSpiritReply> {
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
