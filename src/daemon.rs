use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

use nota_codec::{Decoder, NotaDecode, NotaTransparent};
use owner_signal_persona_spirit::{
    Frame as OwnerFrame, FrameBody as OwnerFrameBody, Operation as OwnerOperation,
    Reply as OwnerReply,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload,
    RequestRejectionReason, SessionEpoch, SubReply,
};
use signal_persona_spirit::{
    Frame, FrameBody, Operation as WorkingOperation, Reply as WorkingReply,
};
use signal_version_handover::{
    Frame as UpgradeFrame, FrameBody as UpgradeFrameBody, Operation as UpgradeOperation,
    Reply as UpgradeReply,
};

use crate::{
    Error, Result, StoreLocation,
    actors::{policy::BootstrapPolicySource, root::SpiritRoot},
};

const DEFAULT_MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, nota_codec::NotaRecord)]
pub struct DaemonConfiguration {
    pub ordinary_socket_path: SocketPath,
    pub owner_socket_path: SocketPath,
    pub upgrade_socket_path: SocketPath,
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
pub struct FrameCodec {
    maximum_frame_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerFrameCodec {
    maximum_frame_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeFrameCodec {
    maximum_frame_bytes: usize,
}

pub struct DaemonRuntime {
    configuration: DaemonConfiguration,
}

pub struct BoundDaemon {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    upgrade_socket: SocketPath,
    ordinary_listener: UnixListener,
    owner_listener: UnixListener,
    upgrade_listener: UnixListener,
    runtime: Arc<tokio::runtime::Runtime>,
    root: kameo::actor::ActorRef<SpiritRoot>,
    codec: FrameCodec,
    owner_codec: OwnerFrameCodec,
    upgrade_codec: UpgradeFrameCodec,
    public_sockets: PublicSockets,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalClient {
    socket: SocketPath,
    codec: FrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerSignalClient {
    socket: SocketPath,
    codec: OwnerFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeSignalClient {
    socket: SocketPath,
    codec: UpgradeFrameCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRequest {
    exchange: ExchangeIdentifier,
    request: signal_frame::Request<WorkingOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedOwnerRequest {
    exchange: ExchangeIdentifier,
    request: signal_frame::Request<OwnerOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedUpgradeRequest {
    exchange: ExchangeIdentifier,
    request: signal_frame::Request<UpgradeOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedExchange {
    reply: Reply<WorkingReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedOwnerExchange {
    reply: Reply<OwnerReply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedUpgradeExchange {
    reply: Reply<UpgradeReply>,
}

#[derive(Debug, Clone)]
struct PublicSockets {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    accepting: Arc<AtomicBool>,
}

impl DaemonConfiguration {
    pub fn new(
        ordinary_socket_path: SocketPath,
        owner_socket_path: SocketPath,
        upgrade_socket_path: SocketPath,
        store_path: StorePath,
        socket_mode: SocketMode,
    ) -> Self {
        Self {
            ordinary_socket_path,
            owner_socket_path,
            upgrade_socket_path,
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

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAXIMUM_FRAME_BYTES)
    }
}

impl Default for OwnerFrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAXIMUM_FRAME_BYTES)
    }
}

impl Default for UpgradeFrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAXIMUM_FRAME_BYTES)
    }
}

impl FrameCodec {
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

    pub fn request_frame(&self, request: WorkingOperation) -> Frame {
        Frame::new(FrameBody::Request {
            exchange: self.exchange(),
            request: request.into_request(),
        })
    }

    pub fn reply_frame(&self, exchange: ExchangeIdentifier, reply: Reply<WorkingReply>) -> Frame {
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

    pub fn reply_from_frame(&self, frame: Frame) -> Result<Reply<WorkingReply>> {
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

impl OwnerFrameCodec {
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

    pub fn request_frame(&self, request: OwnerOperation) -> OwnerFrame {
        OwnerFrame::new(OwnerFrameBody::Request {
            exchange: self.exchange(),
            request: request.into_request(),
        })
    }

    pub fn reply_frame(
        &self,
        exchange: ExchangeIdentifier,
        reply: Reply<OwnerReply>,
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

    pub fn reply_from_frame(&self, frame: OwnerFrame) -> Result<Reply<OwnerReply>> {
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

impl UpgradeFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, stream: &mut UnixStream) -> Result<UpgradeFrame> {
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
        UpgradeFrame::decode_length_prefixed(&bytes).map_err(Error::signal_frame)
    }

    pub fn write_frame(&self, stream: &mut UnixStream, frame: &UpgradeFrame) -> Result<()> {
        let bytes = frame
            .encode_length_prefixed()
            .map_err(Error::signal_frame)?;
        stream.write_all(&bytes).map_err(Error::input_output)?;
        stream.flush().map_err(Error::input_output)
    }

    pub fn request_frame(&self, request: UpgradeOperation) -> UpgradeFrame {
        UpgradeFrame::new(UpgradeFrameBody::Request {
            exchange: self.exchange(),
            request: request.into_request(),
        })
    }

    pub fn reply_frame(
        &self,
        exchange: ExchangeIdentifier,
        reply: Reply<UpgradeReply>,
    ) -> UpgradeFrame {
        UpgradeFrame::new(UpgradeFrameBody::Reply { exchange, reply })
    }

    pub fn request_from_frame(&self, frame: UpgradeFrame) -> Result<ReceivedUpgradeRequest> {
        match frame.into_body() {
            UpgradeFrameBody::Request { exchange, request } => {
                Ok(ReceivedUpgradeRequest { exchange, request })
            }
            other => Err(Error::UnexpectedFrame {
                expected: "upgrade request",
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn reply_from_frame(&self, frame: UpgradeFrame) -> Result<Reply<UpgradeReply>> {
        match frame.into_body() {
            UpgradeFrameBody::Reply { reply, .. } => Ok(reply),
            other => Err(Error::UnexpectedFrame {
                expected: "upgrade reply",
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
        SocketBinding::bind(
            &self.configuration.upgrade_socket_path,
            self.configuration.socket_mode,
        )?;
        let ordinary_listener =
            UnixListener::bind(self.configuration.ordinary_socket_path.as_path())
                .map_err(Error::input_output)?;
        let owner_listener = UnixListener::bind(self.configuration.owner_socket_path.as_path())
            .map_err(Error::input_output)?;
        let upgrade_listener = UnixListener::bind(self.configuration.upgrade_socket_path.as_path())
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
        std::fs::set_permissions(
            self.configuration.upgrade_socket_path.as_path(),
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
            ordinary_socket: self.configuration.ordinary_socket_path.clone(),
            owner_socket: self.configuration.owner_socket_path.clone(),
            upgrade_socket: self.configuration.upgrade_socket_path,
            ordinary_listener,
            owner_listener,
            upgrade_listener,
            runtime,
            root,
            codec: FrameCodec::default(),
            owner_codec: OwnerFrameCodec::default(),
            upgrade_codec: UpgradeFrameCodec::default(),
            public_sockets: PublicSockets::open(
                self.configuration.ordinary_socket_path.clone(),
                self.configuration.owner_socket_path.clone(),
            ),
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

    pub fn upgrade_socket_path(&self) -> &Path {
        self.upgrade_socket.as_path()
    }

    pub fn serve_one(&mut self) -> Result<ServedExchange> {
        let (mut stream, _address) = self
            .ordinary_listener
            .accept()
            .map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = if self.public_sockets.is_accepting() {
            self.reply_to_request(received.request)?
        } else {
            Reply::rejected(RequestRejectionReason::Internal)
        };
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedExchange::new(reply))
    }

    pub fn serve_owner_one(&mut self) -> Result<ServedOwnerExchange> {
        let (mut stream, _address) = self.owner_listener.accept().map_err(Error::input_output)?;
        let frame = self.owner_codec.read_frame(&mut stream)?;
        let received = self.owner_codec.request_from_frame(frame)?;
        let reply = if self.public_sockets.is_accepting() {
            self.reply_to_owner_request(received.request)?
        } else {
            Reply::rejected(RequestRejectionReason::Internal)
        };
        let frame = self
            .owner_codec
            .reply_frame(received.exchange, reply.clone());
        self.owner_codec.write_frame(&mut stream, &frame)?;
        Ok(ServedOwnerExchange::new(reply))
    }

    pub fn serve_upgrade_one(&mut self) -> Result<ServedUpgradeExchange> {
        let (mut stream, _address) = self
            .upgrade_listener
            .accept()
            .map_err(Error::input_output)?;
        let frame = self.upgrade_codec.read_frame(&mut stream)?;
        let received = self.upgrade_codec.request_from_frame(frame)?;
        let reply = self.reply_to_upgrade_request(received.request)?;
        let frame = self
            .upgrade_codec
            .reply_frame(received.exchange, reply.clone());
        self.upgrade_codec.write_frame(&mut stream, &frame)?;
        Ok(ServedUpgradeExchange::new(reply))
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

    pub fn serve_upgrade_count(mut self, count: usize) -> Result<Vec<ServedUpgradeExchange>> {
        let result = (0..count)
            .map(|_| self.serve_upgrade_one())
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
            self.public_sockets.clone(),
        );
        let owner = OwnerSocketServer::new(
            self.owner_listener
                .try_clone()
                .map_err(Error::input_output)?,
            self.root.clone(),
            self.runtime.clone(),
            self.owner_codec,
            self.public_sockets.clone(),
        );
        let upgrade = UpgradeSocketServer::new(
            self.upgrade_listener
                .try_clone()
                .map_err(Error::input_output)?,
            self.root.clone(),
            self.runtime.clone(),
            self.upgrade_codec,
            self.public_sockets.clone(),
        );
        let ordinary_handle = thread::spawn(move || ordinary.serve_forever());
        let owner_handle = thread::spawn(move || owner.serve_forever());
        let upgrade_result = upgrade.serve_forever();
        let ordinary_result = ordinary_handle
            .join()
            .map_err(|_| Error::actor_runtime("ordinary socket thread panicked"))?;
        let owner_result = owner_handle
            .join()
            .map_err(|_| Error::actor_runtime("owner socket thread panicked"))?;
        upgrade_result.and(owner_result).and(ordinary_result)
    }

    pub fn shutdown(self) -> Result<()> {
        let stop = self.runtime.block_on(SpiritRoot::stop(self.root));
        let remove_ordinary = SocketBinding::remove(&self.ordinary_socket);
        let remove_owner = SocketBinding::remove(&self.owner_socket);
        let remove_upgrade = SocketBinding::remove(&self.upgrade_socket);
        match (stop, remove_ordinary, remove_owner, remove_upgrade) {
            (Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
            (Err(error), _, _, _) => Err(error),
            (Ok(()), Err(error), _, _) => Err(error),
            (Ok(()), Ok(()), Err(error), _) => Err(error),
            (Ok(()), Ok(()), Ok(()), Err(error)) => Err(error),
        }
    }

    fn reply_to_request(
        &self,
        request: signal_frame::Request<WorkingOperation>,
    ) -> Result<Reply<WorkingReply>> {
        OrdinaryExchangeHandler::new(self.root.clone(), self.runtime.clone())
            .reply_to_request(request)
    }

    fn reply_to_owner_request(
        &self,
        request: signal_frame::Request<OwnerOperation>,
    ) -> Result<Reply<OwnerReply>> {
        OwnerExchangeHandler::new(self.root.clone(), self.runtime.clone()).reply_to_request(request)
    }

    fn reply_to_upgrade_request(
        &self,
        request: signal_frame::Request<UpgradeOperation>,
    ) -> Result<Reply<UpgradeReply>> {
        UpgradeExchangeHandler::new(
            self.root.clone(),
            self.runtime.clone(),
            self.public_sockets.clone(),
        )
        .reply_to_request(request)
    }
}

struct SocketServer {
    listener: UnixListener,
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    codec: FrameCodec,
    public_sockets: PublicSockets,
}

struct OwnerSocketServer {
    listener: UnixListener,
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    codec: OwnerFrameCodec,
    public_sockets: PublicSockets,
}

struct UpgradeSocketServer {
    listener: UnixListener,
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    codec: UpgradeFrameCodec,
    public_sockets: PublicSockets,
}

struct OrdinaryExchangeHandler {
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
}

struct OwnerExchangeHandler {
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
}

struct UpgradeExchangeHandler {
    root: kameo::actor::ActorRef<SpiritRoot>,
    runtime: Arc<tokio::runtime::Runtime>,
    public_sockets: PublicSockets,
}

impl SocketServer {
    fn new(
        listener: UnixListener,
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
        codec: FrameCodec,
        public_sockets: PublicSockets,
    ) -> Self {
        Self {
            listener,
            root,
            runtime,
            codec,
            public_sockets,
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
        let reply = if self.public_sockets.is_accepting() {
            OrdinaryExchangeHandler::new(self.root.clone(), self.runtime.clone())
                .reply_to_request(received.request)?
        } else {
            Reply::rejected(RequestRejectionReason::Internal)
        };
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
        codec: OwnerFrameCodec,
        public_sockets: PublicSockets,
    ) -> Self {
        Self {
            listener,
            root,
            runtime,
            codec,
            public_sockets,
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
        let reply = if self.public_sockets.is_accepting() {
            OwnerExchangeHandler::new(self.root.clone(), self.runtime.clone())
                .reply_to_request(received.request)?
        } else {
            Reply::rejected(RequestRejectionReason::Internal)
        };
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedOwnerExchange::new(reply))
    }
}

impl UpgradeSocketServer {
    fn new(
        listener: UnixListener,
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
        codec: UpgradeFrameCodec,
        public_sockets: PublicSockets,
    ) -> Self {
        Self {
            listener,
            root,
            runtime,
            codec,
            public_sockets,
        }
    }

    fn serve_forever(self) -> Result<()> {
        loop {
            if let Err(error) = self.serve_one() {
                eprintln!("persona-spirit-daemon upgrade client error: {error}");
            }
        }
    }

    fn serve_one(&self) -> Result<ServedUpgradeExchange> {
        let (mut stream, _address) = self.listener.accept().map_err(Error::input_output)?;
        let frame = self.codec.read_frame(&mut stream)?;
        let received = self.codec.request_from_frame(frame)?;
        let reply = UpgradeExchangeHandler::new(
            self.root.clone(),
            self.runtime.clone(),
            self.public_sockets.clone(),
        )
        .reply_to_request(received.request)?;
        let frame = self.codec.reply_frame(received.exchange, reply.clone());
        self.codec.write_frame(&mut stream, &frame)?;
        Ok(ServedUpgradeExchange::new(reply))
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
        request: signal_frame::Request<WorkingOperation>,
    ) -> Result<Reply<WorkingReply>> {
        let reply = self
            .runtime
            .block_on(async {
                self.root
                    .ask(crate::actors::root::SubmitFrameRequest { request })
                    .await
            })
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        Ok(reply.into_reply())
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
        request: signal_frame::Request<OwnerOperation>,
    ) -> Result<Reply<OwnerReply>> {
        let replies = request
            .payloads
            .into_iter()
            .map(|request| self.reply_to_operation(request))
            .collect::<Result<Vec<_>>>()?;
        Ok(Reply::committed(
            NonEmpty::try_from_vec(replies).expect("request is non-empty"),
        ))
    }

    fn reply_to_operation(&self, request: OwnerOperation) -> Result<SubReply<OwnerReply>> {
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

impl UpgradeExchangeHandler {
    fn new(
        root: kameo::actor::ActorRef<SpiritRoot>,
        runtime: Arc<tokio::runtime::Runtime>,
        public_sockets: PublicSockets,
    ) -> Self {
        Self {
            root,
            runtime,
            public_sockets,
        }
    }

    fn reply_to_request(
        &self,
        request: signal_frame::Request<UpgradeOperation>,
    ) -> Result<Reply<UpgradeReply>> {
        let replies = request
            .payloads
            .into_iter()
            .map(|request| self.reply_to_operation(request))
            .collect::<Result<Vec<_>>>()?;
        Ok(Reply::committed(
            NonEmpty::try_from_vec(replies).expect("request is non-empty"),
        ))
    }

    fn reply_to_operation(&self, request: UpgradeOperation) -> Result<SubReply<UpgradeReply>> {
        let closes_public_sockets = matches!(request, UpgradeOperation::HandoverCompleted(_));
        let reply = self
            .runtime
            .block_on(async {
                self.root
                    .ask(crate::actors::root::SubmitUpgradeRequest { request })
                    .await
            })
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        let reply = reply.into_reply();
        if closes_public_sockets && matches!(reply, UpgradeReply::HandoverFinalized(_)) {
            self.public_sockets.close();
        }
        Ok(SubReply::Ok(reply))
    }
}

impl PublicSockets {
    fn open(ordinary_socket: SocketPath, owner_socket: SocketPath) -> Self {
        Self {
            ordinary_socket,
            owner_socket,
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    fn close(&self) {
        self.accepting.store(false, Ordering::SeqCst);
        let _ = SocketBinding::remove(&self.ordinary_socket);
        let _ = SocketBinding::remove(&self.owner_socket);
    }
}

impl SignalClient {
    pub fn new(socket: SocketPath) -> Self {
        Self {
            socket,
            codec: FrameCodec::default(),
        }
    }

    pub fn submit(&self, request: WorkingOperation) -> Result<WorkingReply> {
        let mut stream = UnixStream::connect(self.socket.as_path()).map_err(Error::input_output)?;
        let frame = self.codec.request_frame(request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.reply_payload(self.codec.reply_from_frame(reply)?)
    }

    fn reply_payload(&self, reply: Reply<WorkingReply>) -> Result<WorkingReply> {
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

impl OwnerSignalClient {
    pub fn new(socket: SocketPath) -> Self {
        Self {
            socket,
            codec: OwnerFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: OwnerOperation) -> Result<OwnerReply> {
        let mut stream = UnixStream::connect(self.socket.as_path()).map_err(Error::input_output)?;
        let frame = self.codec.request_frame(request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.reply_payload(self.codec.reply_from_frame(reply)?)
    }

    fn reply_payload(&self, reply: Reply<OwnerReply>) -> Result<OwnerReply> {
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

impl UpgradeSignalClient {
    pub fn new(socket: SocketPath) -> Self {
        Self {
            socket,
            codec: UpgradeFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: UpgradeOperation) -> Result<UpgradeReply> {
        let mut stream = UnixStream::connect(self.socket.as_path()).map_err(Error::input_output)?;
        let frame = self.codec.request_frame(request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.reply_payload(self.codec.reply_from_frame(reply)?)
    }

    fn reply_payload(&self, reply: Reply<UpgradeReply>) -> Result<UpgradeReply> {
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::UnexpectedFrame {
                    expected: "accepted upgrade operation reply",
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
    fn new(reply: Reply<WorkingReply>) -> Self {
        Self { reply }
    }

    pub fn reply(&self) -> &Reply<WorkingReply> {
        &self.reply
    }
}

impl ServedOwnerExchange {
    fn new(reply: Reply<OwnerReply>) -> Self {
        Self { reply }
    }

    pub fn reply(&self) -> &Reply<OwnerReply> {
        &self.reply
    }
}

impl ServedUpgradeExchange {
    fn new(reply: Reply<UpgradeReply>) -> Self {
        Self { reply }
    }

    pub fn reply(&self) -> &Reply<UpgradeReply> {
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
