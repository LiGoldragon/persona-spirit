use crate::{Error, Result, SingleArgument, SpiritStore, StoreLocation};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_persona_spirit::{
    OperationKind, RequestUnimplemented, SpiritReply, SpiritRequest, UnimplementedReason,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritClient {
    request: SingleArgument,
    store: StoreLocation,
}

impl SpiritClient {
    pub fn from_argument(request: SingleArgument) -> Self {
        Self {
            request,
            store: StoreLocation::from_environment(),
        }
    }

    pub fn with_store(request: SingleArgument, store: StoreLocation) -> Self {
        Self { request, store }
    }

    pub fn run(&self) -> Result<()> {
        println!("{}", self.reply_text()?);
        Ok(())
    }

    pub fn reply_text(&self) -> Result<String> {
        SpiritRuntime::open(&self.store)?.reply_text(self.request.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonRuntime {
    configuration: SingleArgument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritRequestText {
    text: String,
}

pub struct SpiritRuntime {
    store: SpiritStore,
}

impl SpiritRequestText {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn reply_text(&self) -> Result<String> {
        let request = self.decode_request()?;
        SpiritReplyText::new(SpiritReply::RequestUnimplemented(RequestUnimplemented {
            operation: request.operation_kind(),
            reason: UnimplementedReason::NotBuiltYet,
        }))
        .encode()
    }

    pub fn decode_request(&self) -> Result<SpiritRequest> {
        let mut decoder = Decoder::new(&self.text);
        let request = SpiritRequest::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        SpiritRequestEnd::new(&mut decoder).expect()?;
        Ok(request)
    }
}

impl SpiritRuntime {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Ok(Self {
            store: SpiritStore::open(store)?,
        })
    }

    pub fn reply_text(&self, text: impl Into<String>) -> Result<String> {
        let request = SpiritRequestText::new(text).decode_request()?;
        SpiritReplyText::new(self.handle_request(request)?).encode()
    }

    pub fn handle_request(&self, request: SpiritRequest) -> Result<SpiritReply> {
        match request {
            SpiritRequest::Entry(entry) => {
                Ok(SpiritReply::RecordAccepted(self.store.assert_entry(entry)?))
            }
            SpiritRequest::RecordObservation(observation) => {
                self.store.observe_records(observation)
            }
            other => Ok(SpiritReply::RequestUnimplemented(RequestUnimplemented {
                operation: other.operation_kind(),
                reason: Self::unimplemented_reason(other.operation_kind()),
            })),
        }
    }

    fn unimplemented_reason(operation: OperationKind) -> UnimplementedReason {
        match operation {
            OperationKind::Statement
            | OperationKind::StateObservation
            | OperationKind::QuestionPending
            | OperationKind::SubscribeState
            | OperationKind::StateSubscriptionRetraction
            | OperationKind::SubscribeRecords
            | OperationKind::RecordSubscriptionRetraction => UnimplementedReason::NotBuiltYet,
            OperationKind::Entry | OperationKind::RecordObservation => {
                UnimplementedReason::IntegrationNotLanded
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiritReplyText {
    reply: SpiritReply,
}

impl SpiritReplyText {
    pub fn new(reply: SpiritReply) -> Self {
        Self { reply }
    }

    pub fn encode(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.reply
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        Ok(encoder.into_string())
    }
}

struct SpiritRequestEnd<'decoder, 'input> {
    decoder: &'decoder mut Decoder<'input>,
}

impl<'decoder, 'input> SpiritRequestEnd<'decoder, 'input> {
    fn new(decoder: &'decoder mut Decoder<'input>) -> Self {
        Self { decoder }
    }

    fn expect(&mut self) -> Result<()> {
        if let Some(token) = self
            .decoder
            .peek_token()
            .map_err(Error::invalid_spirit_request)?
        {
            Err(Error::InvalidSpiritRequest {
                reason: format!("expected end of input, got {token:?}"),
            })
        } else {
            Ok(())
        }
    }
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
