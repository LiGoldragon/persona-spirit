use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use meta_signal_spirit::{
    Drain, DrainedAndStopped, Generation, IdentityName, IdentityRegistered, IdentityRetired,
    Operation as MetaOperation, Registration, Reply as MetaReply, RequestUnimplemented, Retirement,
    Started, UnimplementedReason,
};

use super::policy;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct MetaPlane {
    lifecycle: LifecycleState,
    identities: Vec<IdentityName>,
    policy: ActorRef<policy::PolicyPlane>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleState {
    generation: Option<Generation>,
}

#[derive(Clone)]
pub struct Arguments {
    pub lifecycle: LifecycleState,
    pub policy: ActorRef<policy::PolicyPlane>,
}

pub struct RouteMetaRequest {
    pub request: MetaOperation,
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct MetaPipelineReply {
    pub reply: MetaReply,
    pub trace: ActorTrace,
}

impl MetaPlane {
    fn new(lifecycle: LifecycleState, policy: ActorRef<policy::PolicyPlane>) -> Self {
        Self {
            lifecycle,
            identities: Vec::new(),
            policy,
        }
    }

    async fn route(&mut self, request: MetaOperation, mut trace: ActorTrace) -> MetaPipelineReply {
        trace.record(TraceNode::META_PLANE, TraceAction::MessageReceived);
        let reply = match request {
            MetaOperation::Start(order) => self.start(order.generation),
            MetaOperation::Drain(order) => self.drain(order),
            MetaOperation::Reload(_order) => {
                return self.reload_policy(trace).await;
            }
            MetaOperation::Register(order) => self.register_identity(order),
            MetaOperation::Retire(order) => self.retire_identity(order),
        };
        trace.record(TraceNode::META_PLANE, TraceAction::MessageReplied);
        MetaPipelineReply { reply, trace }
    }

    fn start(&mut self, generation: Generation) -> MetaReply {
        self.lifecycle.generation = Some(generation);
        MetaReply::Started(Started { generation })
    }

    fn drain(&mut self, _order: Drain) -> MetaReply {
        self.lifecycle.generation = None;
        MetaReply::DrainedAndStopped(DrainedAndStopped {})
    }

    async fn reload_policy(&self, trace: ActorTrace) -> MetaPipelineReply {
        match self
            .policy
            .ask(policy::ReloadBootstrapPolicy { trace })
            .await
        {
            Ok(mut policy) => {
                policy
                    .trace
                    .record(TraceNode::META_PLANE, TraceAction::MessageReplied);
                MetaPipelineReply {
                    reply: policy.reply,
                    trace: policy.trace,
                }
            }
            Err(error) => Self::policy_send_error(error),
        }
    }

    fn register_identity(&mut self, order: Registration) -> MetaReply {
        if !self.identities.contains(&order.name) {
            self.identities.push(order.name.clone());
        }
        MetaReply::IdentityRegistered(IdentityRegistered { name: order.name })
    }

    fn retire_identity(&mut self, order: Retirement) -> MetaReply {
        self.identities.retain(|name| name != &order.name);
        MetaReply::IdentityRetired(IdentityRetired { name: order.name })
    }
}

impl Actor for MetaPlane {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.lifecycle, arguments.policy))
    }
}

impl Message<RouteMetaRequest> for MetaPlane {
    type Reply = MetaPipelineReply;

    async fn handle(
        &mut self,
        message: RouteMetaRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.route(message.request, message.trace).await
    }
}

impl MetaPlane {
    fn policy_send_error<Message>(_error: SendError<Message, Infallible>) -> MetaPipelineReply {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::META_PLANE, TraceAction::MessageReplied);
        MetaPipelineReply {
            reply: MetaReply::RequestUnimplemented(RequestUnimplemented {
                reason: UnimplementedReason::DependencyNotReady,
            }),
            trace,
        }
    }
}
