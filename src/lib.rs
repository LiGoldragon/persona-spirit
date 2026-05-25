pub mod actors;
pub mod daemon;
pub mod error;
pub mod migration;
pub mod observation;
pub mod store;

pub const SPIRIT_RUNTIME_SCHEMA_TEXT: &str = include_str!("../schema/spirit-runtime.schema");

signal_frame::emit_schema!("schema/spirit-runtime.schema");

pub use actors::policy::BootstrapPolicySource;
pub use actors::root::{
    RootFrameReply, RootOperationReply, RootOwnerReply, RootTextReply, RootUpgradeReply,
    SpiritActorRuntime,
};
pub use actors::trace::{ActorTrace, TraceAction, TraceNode};
pub use daemon::{
    BootstrapPolicyPath, BoundDaemon, DaemonConfiguration, DaemonRuntime,
    ServedEngineManagementExchange, ServedExchange, ServedOwnerExchange, ServedUpgradeExchange,
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

pub mod owner {
    pub use crate::daemon::owner::{FrameCodec, SignalClient};
}

pub mod upgrade {
    pub use crate::daemon::upgrade::{FrameCodec, SignalClient};
}
