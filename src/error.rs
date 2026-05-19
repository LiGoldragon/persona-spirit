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
}

pub type Result<T> = std::result::Result<T, Error>;
