pub mod argument;
pub mod error;
pub mod runtime;

pub use argument::SingleArgument;
pub use error::{Error, Result};
pub use runtime::{DaemonRuntime, SpiritClient, SpiritReplyText, SpiritRequestText};
