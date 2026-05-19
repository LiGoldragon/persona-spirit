use thiserror::Error as ThisError;

#[derive(ThisError, Debug, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("{program} expects exactly one NOTA or signal-file argument, found {found}")]
    WrongArgumentCount { program: String, found: usize },

    #[error("{program} accepts NOTA or signal-file input, not flag-style argument {argument}")]
    FlagArgument { program: String, argument: String },

    #[error("{surface} runtime is not implemented yet: {reason}")]
    RuntimeNotImplemented {
        surface: &'static str,
        reason: &'static str,
    },

    #[error("invalid persona-spirit request: {reason}")]
    InvalidSpiritRequest { reason: String },

    #[error("invalid persona-spirit reply: {reason}")]
    InvalidSpiritReply { reason: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn invalid_spirit_request(error: nota_codec::Error) -> Self {
        Self::InvalidSpiritRequest {
            reason: error.to_string(),
        }
    }

    pub fn invalid_spirit_reply(error: nota_codec::Error) -> Self {
        Self::InvalidSpiritReply {
            reason: error.to_string(),
        }
    }
}
