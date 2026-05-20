use kameo::actor::{Actor, ActorRef};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use owner_signal_persona_spirit::{
    Drain, DrainedAndStopped, Generation, IdentityName, IdentityRegistered, IdentityRetired,
    OwnerSpiritReply, OwnerSpiritRequest, Registration, RequestUnimplemented, Retirement, Started,
    UnimplementedReason,
};

use super::policy;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct OwnerPlane {
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

pub struct RouteOwnerRequest {
    pub request: OwnerSpiritRequest,
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct OwnerPipelineReply {
    pub reply: OwnerSpiritReply,
    pub trace: ActorTrace,
}

impl OwnerPlane {
    fn new(lifecycle: LifecycleState, policy: ActorRef<policy::PolicyPlane>) -> Self {
        Self {
            lifecycle,
            identities: Vec::new(),
            policy,
        }
    }

    async fn route(
        &mut self,
        request: OwnerSpiritRequest,
        mut trace: ActorTrace,
    ) -> OwnerPipelineReply {
        trace.record(TraceNode::OWNER_PLANE, TraceAction::MessageReceived);
        let reply = match request {
            OwnerSpiritRequest::Start(order) => self.start(order.generation),
            OwnerSpiritRequest::Drain(order) => self.drain(order),
            OwnerSpiritRequest::Reload(_order) => {
                return self.reload_policy(trace).await;
            }
            OwnerSpiritRequest::Register(order) => self.register_identity(order),
            OwnerSpiritRequest::Retire(order) => self.retire_identity(order),
        };
        trace.record(TraceNode::OWNER_PLANE, TraceAction::MessageReplied);
        OwnerPipelineReply { reply, trace }
    }

    fn start(&mut self, generation: Generation) -> OwnerSpiritReply {
        self.lifecycle.generation = Some(generation);
        OwnerSpiritReply::Started(Started { generation })
    }

    fn drain(&mut self, _order: Drain) -> OwnerSpiritReply {
        self.lifecycle.generation = None;
        OwnerSpiritReply::DrainedAndStopped(DrainedAndStopped {})
    }

    async fn reload_policy(&self, trace: ActorTrace) -> OwnerPipelineReply {
        match self
            .policy
            .ask(policy::ReloadBootstrapPolicy { trace })
            .await
        {
            Ok(mut policy) => {
                policy
                    .trace
                    .record(TraceNode::OWNER_PLANE, TraceAction::MessageReplied);
                OwnerPipelineReply {
                    reply: policy.reply,
                    trace: policy.trace,
                }
            }
            Err(error) => Self::policy_send_error(error),
        }
    }

    fn register_identity(&mut self, order: Registration) -> OwnerSpiritReply {
        if !self.identities.contains(&order.name) {
            self.identities.push(order.name.clone());
        }
        OwnerSpiritReply::IdentityRegistered(IdentityRegistered { name: order.name })
    }

    fn retire_identity(&mut self, order: Retirement) -> OwnerSpiritReply {
        self.identities.retain(|name| name != &order.name);
        OwnerSpiritReply::IdentityRetired(IdentityRetired { name: order.name })
    }
}

impl Actor for OwnerPlane {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.lifecycle, arguments.policy))
    }
}

impl Message<RouteOwnerRequest> for OwnerPlane {
    type Reply = OwnerPipelineReply;

    async fn handle(
        &mut self,
        message: RouteOwnerRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.route(message.request, message.trace).await
    }
}

impl OwnerPlane {
    fn policy_send_error<Message>(_error: SendError<Message, Infallible>) -> OwnerPipelineReply {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::OWNER_PLANE, TraceAction::MessageReplied);
        OwnerPipelineReply {
            reply: OwnerSpiritReply::RequestUnimplemented(RequestUnimplemented {
                reason: UnimplementedReason::DependencyNotReady,
            }),
            trace,
        }
    }
}
