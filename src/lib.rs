pub mod actors;
pub mod argument;
pub mod daemon;
pub mod error;
pub mod observation;
pub mod runtime;
pub mod store;

pub use actors::policy::BootstrapPolicySource;
pub use actors::root::{
    RootFrameReply, RootOperationReply, RootOwnerReply, RootTextReply, RootUpgradeReply,
    SpiritActorRuntime,
};
pub use actors::trace::{ActorTrace, TraceAction, TraceNode};
pub use argument::SingleArgument;
pub use daemon::{
    BootstrapPolicyPath, BoundDaemon, DaemonConfiguration, DaemonRuntime, ServedExchange,
    ServedOwnerExchange, ServedUpgradeExchange, SocketMode, SocketPath, StorePath,
};
pub use error::{Error, Result};
pub use observation::{Command, Effect};
pub use store::{SpiritStore, StoreLocation};

pub mod ordinary {
    pub use crate::daemon::{FrameCodec, SignalClient};
    pub use crate::runtime::{
        Client, CommandLineDispatch, CommandLineSockets, ReplyText, RequestHead, RequestInput,
        RequestText,
    };
}

pub mod owner {
    pub use crate::daemon::{OwnerFrameCodec as FrameCodec, OwnerSignalClient as SignalClient};
    pub use crate::runtime::{OwnerReplyText as ReplyText, OwnerRequestText as RequestText};
}

pub mod upgrade {
    pub use crate::daemon::{UpgradeFrameCodec as FrameCodec, UpgradeSignalClient as SignalClient};
}
