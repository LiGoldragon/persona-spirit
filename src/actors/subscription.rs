use std::collections::BTreeMap;

use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_persona_spirit::{
    RecordSubscription, RecordSubscriptionToken, RecordSummary, SpiritReply, State,
    StateSubscriptionToken, SubscriptionOpened, SubscriptionRetracted, SubscriptionSnapshot,
    SubscriptionToken,
};

use super::pipeline::PipelineReply;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct SubscriptionPlane {
    state: BTreeMap<u64, State>,
    records: BTreeMap<u64, RecordSubscription>,
    identifiers: SubscriptionIdentifiers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionIdentifiers {
    next_state: u64,
    next_record: u64,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub identifiers: SubscriptionIdentifiers,
}

pub struct OpenStateSubscription {
    pub snapshot: State,
    pub trace: ActorTrace,
}

pub struct OpenRecordSubscription {
    pub subscription: RecordSubscription,
    pub snapshot: Vec<RecordSummary>,
    pub trace: ActorTrace,
}

pub struct RetractStateSubscription {
    pub token: StateSubscriptionToken,
    pub trace: ActorTrace,
}

pub struct RetractRecordSubscription {
    pub token: RecordSubscriptionToken,
    pub trace: ActorTrace,
}

impl Default for SubscriptionIdentifiers {
    fn default() -> Self {
        Self {
            next_state: 1,
            next_record: 1,
        }
    }
}

impl SubscriptionPlane {
    fn new(identifiers: SubscriptionIdentifiers) -> Self {
        Self {
            state: BTreeMap::new(),
            records: BTreeMap::new(),
            identifiers,
        }
    }

    fn open_state(&mut self, snapshot: State, mut trace: ActorTrace) -> PipelineReply {
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReceived);
        let token = StateSubscriptionToken {
            identifier: self.identifiers.next_state,
        };
        self.identifiers.next_state += 1;
        self.state.insert(token.identifier, snapshot.clone());
        trace.record(
            TraceNode::SUBSCRIPTION_PLANE,
            TraceAction::SubscriptionOpened,
        );
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::SubscriptionOpened(SubscriptionOpened {
                token: SubscriptionToken::State(token),
                snapshot: SubscriptionSnapshot::State(snapshot),
            }),
            trace,
        )
    }

    fn open_records(
        &mut self,
        subscription: RecordSubscription,
        snapshot: Vec<RecordSummary>,
        mut trace: ActorTrace,
    ) -> PipelineReply {
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReceived);
        let token = RecordSubscriptionToken {
            identifier: self.identifiers.next_record,
        };
        self.identifiers.next_record += 1;
        self.records.insert(token.identifier, subscription);
        trace.record(
            TraceNode::SUBSCRIPTION_PLANE,
            TraceAction::SubscriptionOpened,
        );
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::SubscriptionOpened(SubscriptionOpened {
                token: SubscriptionToken::Records(token),
                snapshot: SubscriptionSnapshot::Records(snapshot),
            }),
            trace,
        )
    }

    fn retract_state(
        &mut self,
        token: StateSubscriptionToken,
        mut trace: ActorTrace,
    ) -> PipelineReply {
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReceived);
        self.state.remove(&token.identifier);
        trace.record(
            TraceNode::SUBSCRIPTION_PLANE,
            TraceAction::SubscriptionRetracted,
        );
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::SubscriptionRetracted(SubscriptionRetracted {
                token: SubscriptionToken::State(token),
            }),
            trace,
        )
    }

    fn retract_records(
        &mut self,
        token: RecordSubscriptionToken,
        mut trace: ActorTrace,
    ) -> PipelineReply {
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReceived);
        self.records.remove(&token.identifier);
        trace.record(
            TraceNode::SUBSCRIPTION_PLANE,
            TraceAction::SubscriptionRetracted,
        );
        trace.record(TraceNode::SUBSCRIPTION_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::SubscriptionRetracted(SubscriptionRetracted {
                token: SubscriptionToken::Records(token),
            }),
            trace,
        )
    }
}

impl Actor for SubscriptionPlane {
    type Args = Arguments;
    type Error = std::convert::Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.identifiers))
    }
}

impl Message<OpenStateSubscription> for SubscriptionPlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: OpenStateSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.open_state(message.snapshot, message.trace)
    }
}

impl Message<OpenRecordSubscription> for SubscriptionPlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: OpenRecordSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.open_records(message.subscription, message.snapshot, message.trace)
    }
}

impl Message<RetractStateSubscription> for SubscriptionPlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: RetractStateSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.retract_state(message.token, message.trace)
    }
}

impl Message<RetractRecordSubscription> for SubscriptionPlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: RetractRecordSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.retract_records(message.token, message.trace)
    }
}
