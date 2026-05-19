use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nota_codec::{Decoder, NotaDecode, NotaRecord};
use owner_signal_persona_spirit::{
    BootstrapPolicyReloaded, OperationKind, OwnerSpiritReply, RequestUnimplemented,
    UnimplementedReason,
};

use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct PolicyPlane {
    source: BootstrapPolicySource,
    policy: Option<BootstrapPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapPolicySource {
    Embedded(&'static str),
    Path(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq, NotaRecord)]
pub struct BootstrapPolicy {
    pub text: String,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub source: BootstrapPolicySource,
}

pub struct ReloadBootstrapPolicy {
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct PolicyPipelineReply {
    pub reply: OwnerSpiritReply,
    pub trace: ActorTrace,
}

impl PolicyPlane {
    fn new(source: BootstrapPolicySource) -> Self {
        let policy = BootstrapPolicy::from_source(&source).ok();
        Self { source, policy }
    }

    fn reload(&mut self, mut trace: ActorTrace) -> PolicyPipelineReply {
        trace.record(TraceNode::POLICY_PLANE, TraceAction::MessageReceived);
        let reply = match BootstrapPolicy::from_source(&self.source) {
            Ok(policy) => {
                self.policy = Some(policy);
                OwnerSpiritReply::BootstrapPolicyReloaded(BootstrapPolicyReloaded {})
            }
            Err(_reason) => OwnerSpiritReply::RequestUnimplemented(RequestUnimplemented {
                operation: OperationKind::ReloadBootstrapPolicyOrder,
                reason: UnimplementedReason::DependencyNotReady,
            }),
        };
        trace.record(TraceNode::POLICY_PLANE, TraceAction::MessageReplied);
        PolicyPipelineReply { reply, trace }
    }
}

impl BootstrapPolicySource {
    pub const fn embedded(value: &'static str) -> Self {
        Self::Embedded(value)
    }

    pub fn path(value: impl Into<PathBuf>) -> Self {
        Self::Path(value.into())
    }

    fn read_text(&self) -> Result<String, String> {
        match self {
            Self::Embedded(text) => Ok((*text).to_string()),
            Self::Path(path) => std::fs::read_to_string(path).map_err(|error| error.to_string()),
        }
    }
}

impl Default for BootstrapPolicySource {
    fn default() -> Self {
        Self::embedded(include_str!("../../bootstrap-policy.nota"))
    }
}

impl BootstrapPolicy {
    fn from_source(source: &BootstrapPolicySource) -> Result<Self, String> {
        Self::from_text(&source.read_text()?)
    }

    fn from_text(text: &str) -> Result<Self, String> {
        let mut decoder = Decoder::new(text);
        let policy = Self::decode(&mut decoder).map_err(|error| error.to_string())?;
        PolicyTextEnd::new(&mut decoder).expect()?;
        Ok(policy)
    }
}

impl Actor for PolicyPlane {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.source))
    }
}

impl Message<ReloadBootstrapPolicy> for PolicyPlane {
    type Reply = PolicyPipelineReply;

    async fn handle(
        &mut self,
        message: ReloadBootstrapPolicy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.reload(message.trace)
    }
}

struct PolicyTextEnd<'decoder, 'input> {
    decoder: &'decoder mut Decoder<'input>,
}

impl<'decoder, 'input> PolicyTextEnd<'decoder, 'input> {
    fn new(decoder: &'decoder mut Decoder<'input>) -> Self {
        Self { decoder }
    }

    fn expect(&mut self) -> Result<(), String> {
        if let Some(token) = self
            .decoder
            .peek_token()
            .map_err(|error| error.to_string())?
        {
            Err(format!("expected end of input, got {token:?}"))
        } else {
            Ok(())
        }
    }
}
