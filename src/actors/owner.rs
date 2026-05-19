use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use owner_signal_persona_spirit::{
    DrainAndStopOrder, DrainedAndStopped, Generation, IdentityName, IdentityRegistered,
    IdentityRetired, OperationKind, OwnerSpiritReply, OwnerSpiritRequest, RegisterIdentity,
    ReloadBootstrapPolicyOrder, RequestUnimplemented, RetireIdentity, Started, UnimplementedReason,
};

use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct OwnerPlane {
    lifecycle: LifecycleState,
    identities: Vec<IdentityName>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleState {
    generation: Option<Generation>,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub lifecycle: LifecycleState,
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
    fn new(lifecycle: LifecycleState) -> Self {
        Self {
            lifecycle,
            identities: Vec::new(),
        }
    }

    fn route(&mut self, request: OwnerSpiritRequest, mut trace: ActorTrace) -> OwnerPipelineReply {
        trace.record(TraceNode::OWNER_PLANE, TraceAction::MessageReceived);
        let reply = match request {
            OwnerSpiritRequest::StartOrder(order) => self.start(order.generation),
            OwnerSpiritRequest::DrainAndStopOrder(order) => self.drain_and_stop(order),
            OwnerSpiritRequest::ReloadBootstrapPolicyOrder(order) => self.reload_policy(order),
            OwnerSpiritRequest::RegisterIdentity(order) => self.register_identity(order),
            OwnerSpiritRequest::RetireIdentity(order) => self.retire_identity(order),
        };
        trace.record(TraceNode::OWNER_PLANE, TraceAction::MessageReplied);
        OwnerPipelineReply { reply, trace }
    }

    fn start(&mut self, generation: Generation) -> OwnerSpiritReply {
        self.lifecycle.generation = Some(generation);
        OwnerSpiritReply::Started(Started { generation })
    }

    fn drain_and_stop(&mut self, _order: DrainAndStopOrder) -> OwnerSpiritReply {
        self.lifecycle.generation = None;
        OwnerSpiritReply::DrainedAndStopped(DrainedAndStopped {})
    }

    fn reload_policy(&self, _order: ReloadBootstrapPolicyOrder) -> OwnerSpiritReply {
        OwnerSpiritReply::RequestUnimplemented(RequestUnimplemented {
            operation: OperationKind::ReloadBootstrapPolicyOrder,
            reason: UnimplementedReason::NotBuiltYet,
        })
    }

    fn register_identity(&mut self, order: RegisterIdentity) -> OwnerSpiritReply {
        if !self.identities.contains(&order.name) {
            self.identities.push(order.name.clone());
        }
        OwnerSpiritReply::IdentityRegistered(IdentityRegistered { name: order.name })
    }

    fn retire_identity(&mut self, order: RetireIdentity) -> OwnerSpiritReply {
        self.identities.retain(|name| name != &order.name);
        OwnerSpiritReply::IdentityRetired(IdentityRetired { name: order.name })
    }
}

impl Actor for OwnerPlane {
    type Args = Arguments;
    type Error = std::convert::Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.lifecycle))
    }
}

impl Message<RouteOwnerRequest> for OwnerPlane {
    type Reply = OwnerPipelineReply;

    async fn handle(
        &mut self,
        message: RouteOwnerRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.route(message.request, message.trace)
    }
}
