pub mod actors;
pub mod argument;
pub mod error;
pub mod runtime;
pub mod store;

pub use actors::root::{RootTextReply, SpiritActorRuntime};
pub use actors::trace::{ActorTrace, TraceAction, TraceNode};
pub use argument::SingleArgument;
pub use error::{Error, Result};
pub use runtime::{DaemonRuntime, SpiritClient, SpiritReplyText, SpiritRequestText};
pub use store::{SpiritStore, StoreLocation};
