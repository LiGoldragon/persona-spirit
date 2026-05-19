use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_persona_spirit::{
    Presence, QuestionSummary, QuestionsObserved, SpiritReply, State, StateObserved,
};

use super::pipeline::PipelineReply;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct StatePlane {
    working: WorkingState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingState {
    state: State,
    questions: Vec<QuestionSummary>,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub working: WorkingState,
}

pub struct ObserveState {
    pub trace: ActorTrace,
}

pub struct ObserveQuestions {
    pub trace: ActorTrace,
}

pub struct ReadStateSnapshot {
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct StateSnapshot {
    pub state: State,
    pub trace: ActorTrace,
}

impl Default for WorkingState {
    fn default() -> Self {
        Self {
            state: State {
                presence: Presence::Absent,
                focus: None,
            },
            questions: Vec::new(),
        }
    }
}

impl StatePlane {
    fn new(working: WorkingState) -> Self {
        Self { working }
    }

    fn observe_state(&self, mut trace: ActorTrace) -> PipelineReply {
        trace.record(TraceNode::STATE_PLANE, TraceAction::MessageReceived);
        trace.record(TraceNode::STATE_PLANE, TraceAction::RecordsRead);
        trace.record(TraceNode::STATE_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::StateObserved(StateObserved {
                state: self.working.state.clone(),
            }),
            trace,
        )
    }

    fn observe_questions(&self, mut trace: ActorTrace) -> PipelineReply {
        trace.record(TraceNode::STATE_PLANE, TraceAction::MessageReceived);
        trace.record(TraceNode::STATE_PLANE, TraceAction::RecordsRead);
        trace.record(TraceNode::STATE_PLANE, TraceAction::MessageReplied);
        PipelineReply::new(
            SpiritReply::QuestionsObserved(QuestionsObserved {
                questions: self.working.questions.clone(),
            }),
            trace,
        )
    }

    fn read_state_snapshot(&self, mut trace: ActorTrace) -> StateSnapshot {
        trace.record(TraceNode::STATE_PLANE, TraceAction::MessageReceived);
        trace.record(TraceNode::STATE_PLANE, TraceAction::RecordsRead);
        StateSnapshot {
            state: self.working.state.clone(),
            trace,
        }
    }
}

impl Actor for StatePlane {
    type Args = Arguments;
    type Error = std::convert::Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.working))
    }
}

impl Message<ObserveState> for StatePlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: ObserveState,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_state(message.trace)
    }
}

impl Message<ObserveQuestions> for StatePlane {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: ObserveQuestions,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_questions(message.trace)
    }
}

impl Message<ReadStateSnapshot> for StatePlane {
    type Reply = StateSnapshot;

    async fn handle(
        &mut self,
        message: ReadStateSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.read_state_snapshot(message.trace)
    }
}
