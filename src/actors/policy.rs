use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use meta_signal_spirit::{
    BootstrapPolicyReloaded, Reply as MetaReply, RequestUnimplemented, UnimplementedReason,
};
use nota_next::{NotaDecode, NotaEncode, NotaSource};

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

#[derive(Debug, Clone, PartialEq, Eq, NotaEncode, NotaDecode)]
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
    pub reply: MetaReply,
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
                MetaReply::BootstrapPolicyReloaded(BootstrapPolicyReloaded {})
            }
            Err(_reason) => MetaReply::RequestUnimplemented(RequestUnimplemented {
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
        NotaSource::new(text)
            .parse::<Self>()
            .map_err(|error| error.to_string())
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
