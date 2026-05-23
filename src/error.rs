use signal_frame::{
    BatchErrorClassification, BatchFailureReason, CommitStatus, RetryClassification,
};
use thiserror::Error as ThisError;

#[derive(ThisError, Debug, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("{program} expects exactly one NOTA or signal-file argument, found {found}")]
    WrongArgumentCount { program: String, found: usize },

    #[error("{program} accepts NOTA or signal-file input, not flag-style argument {argument}")]
    FlagArgument { program: String, argument: String },

    #[error("persona-spirit requires PERSONA_SPIRIT_SOCKET to reach the daemon")]
    MissingSpiritSocket,

    #[error("persona-spirit requires PERSONA_SPIRIT_OWNER_SOCKET to reach the owner daemon socket")]
    MissingOwnerSpiritSocket,

    #[error("persona-spirit cannot route command-line request: {reason}")]
    CommandLineRoute { reason: String },

    #[error("{surface} runtime is not implemented yet: {reason}")]
    RuntimeNotImplemented {
        surface: &'static str,
        reason: &'static str,
    },

    #[error("invalid persona-spirit request: {reason}")]
    InvalidSpiritRequest { reason: String },

    #[error("invalid persona-spirit reply: {reason}")]
    InvalidSpiritReply { reason: String },

    #[error("invalid persona-spirit daemon configuration: {reason}")]
    InvalidDaemonConfiguration { reason: String },

    #[error("persona-spirit input/output error: {reason}")]
    InputOutput { reason: String },

    #[error("persona-spirit signal frame error: {reason}")]
    SignalFrame { reason: String },

    #[error("persona-spirit frame too large: found {found} bytes, limit {limit}")]
    FrameTooLarge { found: usize, limit: usize },

    #[error("unexpected persona-spirit signal frame: expected {expected}, got {got}")]
    UnexpectedFrame { expected: &'static str, got: String },

    #[error("persona-spirit request rejected before execution: {reason}")]
    RequestRejected { reason: String },

    #[error("persona-spirit does not yet support atomic batches with {operation_count} operations")]
    UnsupportedAtomicBatch { operation_count: usize },

    #[error(
        "persona-spirit does not yet support atomic operation plans with {command_count} commands"
    )]
    UnsupportedAtomicOperationPlan { command_count: usize },

    #[error("persona-spirit store error: {reason}")]
    SpiritStore { reason: String },

    #[error("persona-spirit actor runtime error: {reason}")]
    ActorRuntime { reason: String },
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

    pub fn invalid_daemon_configuration(error: nota_codec::Error) -> Self {
        Self::InvalidDaemonConfiguration {
            reason: error.to_string(),
        }
    }

    pub fn input_output(error: std::io::Error) -> Self {
        Self::InputOutput {
            reason: error.to_string(),
        }
    }

    pub fn signal_frame(error: impl std::fmt::Display) -> Self {
        Self::SignalFrame {
            reason: error.to_string(),
        }
    }

    pub fn command_line_route(error: signal_frame::CommandLineRouteError) -> Self {
        Self::CommandLineRoute {
            reason: error.to_string(),
        }
    }

    pub fn spirit_store(error: sema_engine::Error) -> Self {
        Self::SpiritStore {
            reason: error.to_string(),
        }
    }

    pub fn actor_runtime(reason: impl Into<String>) -> Self {
        Self::ActorRuntime {
            reason: reason.into(),
        }
    }
}

impl From<signal_frame::SingleArgumentError> for Error {
    fn from(error: signal_frame::SingleArgumentError) -> Self {
        match error {
            signal_frame::SingleArgumentError::WrongArgumentCount { program, found } => {
                Self::WrongArgumentCount { program, found }
            }
            signal_frame::SingleArgumentError::FlagArgument { program, argument } => {
                Self::FlagArgument { program, argument }
            }
        }
    }
}

impl From<signal_frame::CommandLineError> for Error {
    fn from(error: signal_frame::CommandLineError) -> Self {
        match error {
            signal_frame::CommandLineError::Argument(error) => Self::from(error),
            signal_frame::CommandLineError::MissingSocket { variable } => match variable.as_str() {
                "PERSONA_SPIRIT_SOCKET" => Self::MissingSpiritSocket,
                "PERSONA_SPIRIT_OWNER_SOCKET" => Self::MissingOwnerSpiritSocket,
                _ => Self::InputOutput {
                    reason: format!("missing socket environment variable {variable}"),
                },
            },
            signal_frame::CommandLineError::Route { reason } => Self::CommandLineRoute { reason },
            signal_frame::CommandLineError::InvalidRequest { reason } => {
                Self::InvalidSpiritRequest { reason }
            }
            signal_frame::CommandLineError::InvalidReply { reason } => {
                Self::InvalidSpiritReply { reason }
            }
            signal_frame::CommandLineError::InputOutput { reason } => Self::InputOutput { reason },
            signal_frame::CommandLineError::SignalFrame { reason } => Self::SignalFrame { reason },
            signal_frame::CommandLineError::FrameTooLarge { found, limit } => {
                Self::FrameTooLarge { found, limit }
            }
            signal_frame::CommandLineError::UnexpectedFrame { expected, got } => {
                Self::UnexpectedFrame { expected, got }
            }
            signal_frame::CommandLineError::RequestRejected { reason } => {
                Self::RequestRejected { reason }
            }
        }
    }
}

impl BatchErrorClassification for Error {
    fn batch_failure_reason(&self) -> BatchFailureReason {
        match self {
            Self::ActorRuntime { .. } | Self::InputOutput { .. } => {
                BatchFailureReason::EngineUnavailable
            }
            _ => BatchFailureReason::EngineRejected,
        }
    }

    fn retry_classification(&self) -> RetryClassification {
        match self {
            Self::ActorRuntime { .. } | Self::InputOutput { .. } => RetryClassification::Unknown,
            Self::UnsupportedAtomicBatch { .. } | Self::UnsupportedAtomicOperationPlan { .. } => {
                RetryClassification::NotRetryable
            }
            _ => RetryClassification::NotRetryable,
        }
    }

    fn commit_status(&self) -> CommitStatus {
        match self {
            Self::UnsupportedAtomicBatch { .. }
            | Self::UnsupportedAtomicOperationPlan { .. }
            | Self::InvalidSpiritRequest { .. }
            | Self::InvalidSpiritReply { .. }
            | Self::InvalidDaemonConfiguration { .. }
            | Self::RequestRejected { .. }
            | Self::RuntimeNotImplemented { .. }
            | Self::SignalFrame { .. }
            | Self::FrameTooLarge { .. }
            | Self::UnexpectedFrame { .. }
            | Self::MissingSpiritSocket
            | Self::MissingOwnerSpiritSocket
            | Self::CommandLineRoute { .. }
            | Self::WrongArgumentCount { .. }
            | Self::FlagArgument { .. } => CommitStatus::NotCommitted,
            Self::InputOutput { .. } | Self::ActorRuntime { .. } | Self::SpiritStore { .. } => {
                CommitStatus::Unknown
            }
        }
    }
}
