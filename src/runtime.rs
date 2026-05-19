use crate::{Error, Result, SingleArgument};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritClient {
    request: SingleArgument,
}

impl SpiritClient {
    pub fn from_argument(request: SingleArgument) -> Self {
        Self { request }
    }

    pub fn run(&self) -> Result<()> {
        let _request_text = self.request.as_str();
        Err(Error::RuntimeNotImplemented {
            surface: "persona-spirit",
            reason: "CLI-to-daemon transport and spirit daemon socket are not implemented",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonRuntime {
    configuration: SingleArgument,
}

impl DaemonRuntime {
    pub fn from_argument(configuration: SingleArgument) -> Self {
        Self { configuration }
    }

    pub fn run(&self) -> Result<()> {
        let _configuration_text = self.configuration.as_str();
        Err(Error::RuntimeNotImplemented {
            surface: "persona-spirit-daemon",
            reason: "Kameo actor tree, sema-engine state, and sockets are not implemented",
        })
    }
}
