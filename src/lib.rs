pub mod actors;
pub mod command_line;
pub mod daemon;
pub mod error;
pub mod migration;
pub mod observation;
pub mod store;

pub use actors::policy::BootstrapPolicySource;
pub use actors::root::{
    RootFrameReply, RootMetaReply, RootOperationReply, RootTextReply, RootUpgradeReply,
    SpiritActorRuntime,
};
pub use actors::trace::{ActorTrace, TraceAction, TraceNode};
pub use command_line::{CommandLine, CommandLineReply};
pub use daemon::{
    BootstrapPolicyPath, BoundDaemon, DaemonConfiguration, DaemonRuntime,
    ServedEngineManagementExchange, ServedExchange, ServedMetaExchange, ServedUpgradeExchange,
    SocketMode, SocketPath, StorePath,
};
pub use error::{Error, Result};
pub use migration::{MigrationCompleted, MigrationConfiguration, MigrationOutcome};
pub use observation::{Command, Effect};
pub use signal_frame::SingleArgument;
pub use store::{SpiritStore, StoreLocation};

pub mod ordinary {
    pub use crate::daemon::ordinary::{FrameCodec, SignalClient};
}

pub mod meta {
    pub use crate::daemon::meta::{FrameCodec, SignalClient};
}

pub mod upgrade {
    pub use crate::daemon::upgrade::{FrameCodec, SignalClient};
}
