//! Projection from Spirit-local execution records to universal Sema
//! observation labels.
//!
//! The executable records remain local to this component. `signal-sema`
//! receives only payloadless classification labels.

use signal_persona_spirit::{
    Observation, QuestionPending, QuestionsObserved, RecordAccepted, RecordObservation,
    RecordProvenancesObserved, RecordQuery, RecordSubscription, RecordSubscriptionOpened,
    RecordSubscriptionRetracted, RecordSubscriptionToken, RecordsObserved, RequestUnimplemented,
    SpiritObserverFilter, SpiritObserverSubscriptionOpened, SpiritObserverSubscriptionToken,
    SpiritReply, SpiritRequest, StateObservation, StateObserved, StateSubscription,
    StateSubscriptionOpened, StateSubscriptionRetracted, StateSubscriptionToken, Statement,
    Subscription, SubscriptionOpened, SubscriptionRetracted, SubscriptionToken,
};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    ClassifyStatement(Statement),
    AssertEntry(signal_persona_spirit::Entry),
    ReadRecords(RecordObservation),
    ReadState(StateObservation),
    ReadQuestions(QuestionPending),
    OpenStateSubscription(StateSubscription),
    OpenRecordSubscription(RecordSubscription),
    CloseStateSubscription(StateSubscriptionToken),
    CloseRecordSubscription(RecordSubscriptionToken),
    OpenObserverSubscription(SpiritObserverFilter),
    CloseObserverSubscription(SpiritObserverSubscriptionToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    RecordAccepted(RecordAccepted),
    StateObserved(StateObserved),
    RecordsObserved(RecordsObserved),
    RecordProvenancesObserved(RecordProvenancesObserved),
    QuestionsObserved(QuestionsObserved),
    StateSubscriptionOpened(StateSubscriptionOpened),
    RecordSubscriptionOpened(RecordSubscriptionOpened),
    SubscriptionOpened(SubscriptionOpened),
    StateSubscriptionRetracted(StateSubscriptionRetracted),
    RecordSubscriptionRetracted(RecordSubscriptionRetracted),
    SubscriptionRetracted(SubscriptionRetracted),
    ObserverSubscriptionOpened(SpiritObserverSubscriptionOpened),
    RequestUnimplemented(RequestUnimplemented),
}

impl Command {
    pub fn from_request(request: SpiritRequest) -> Option<Self> {
        match request {
            SpiritRequest::State(statement) => Some(Self::ClassifyStatement(statement)),
            SpiritRequest::Record(entry) => Some(Self::AssertEntry(entry)),
            SpiritRequest::Observe(Observation::Records(query)) => {
                Some(Self::ReadRecords(RecordObservation { query }))
            }
            SpiritRequest::Observe(Observation::State(observation)) => {
                Some(Self::ReadState(observation))
            }
            SpiritRequest::Observe(Observation::Questions(pending)) => {
                Some(Self::ReadQuestions(pending))
            }
            SpiritRequest::Watch(Subscription::State(subscription)) => {
                Some(Self::OpenStateSubscription(subscription))
            }
            SpiritRequest::Watch(Subscription::Records(subscription)) => {
                Some(Self::OpenRecordSubscription(subscription))
            }
            SpiritRequest::Unwatch(SubscriptionToken::State(token)) => {
                Some(Self::CloseStateSubscription(token))
            }
            SpiritRequest::Unwatch(SubscriptionToken::Records(token)) => {
                Some(Self::CloseRecordSubscription(token))
            }
            SpiritRequest::Tap(filter) => Some(Self::OpenObserverSubscription(filter)),
            SpiritRequest::Untap(token) => Some(Self::CloseObserverSubscription(token)),
        }
    }
}

impl Effect {
    pub fn from_reply(reply: SpiritReply) -> Self {
        match reply {
            SpiritReply::RecordAccepted(payload) => Self::RecordAccepted(payload),
            SpiritReply::StateObserved(payload) => Self::StateObserved(payload),
            SpiritReply::RecordsObserved(payload) => Self::RecordsObserved(payload),
            SpiritReply::RecordProvenancesObserved(payload) => {
                Self::RecordProvenancesObserved(payload)
            }
            SpiritReply::QuestionsObserved(payload) => Self::QuestionsObserved(payload),
            SpiritReply::StateSubscriptionOpened(payload) => Self::StateSubscriptionOpened(payload),
            SpiritReply::RecordSubscriptionOpened(payload) => {
                Self::RecordSubscriptionOpened(payload)
            }
            SpiritReply::SubscriptionOpened(payload) => Self::SubscriptionOpened(payload),
            SpiritReply::StateSubscriptionRetracted(payload) => {
                Self::StateSubscriptionRetracted(payload)
            }
            SpiritReply::RecordSubscriptionRetracted(payload) => {
                Self::RecordSubscriptionRetracted(payload)
            }
            SpiritReply::SubscriptionRetracted(payload) => Self::SubscriptionRetracted(payload),
            SpiritReply::ObserverSubscriptionOpened(payload) => {
                Self::ObserverSubscriptionOpened(payload)
            }
            SpiritReply::RequestUnimplemented(payload) => Self::RequestUnimplemented(payload),
        }
    }

    pub fn sema_observation_for(&self, command: &Command) -> SemaObservation {
        SemaObservation::from_projection(command, self)
    }
}

impl ToSemaOperation for Command {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::ClassifyStatement(_) | Self::AssertEntry(_) => SemaOperation::Assert,
            Self::ReadRecords(_) | Self::ReadState(_) | Self::ReadQuestions(_) => {
                SemaOperation::Match
            }
            Self::OpenStateSubscription(_)
            | Self::OpenRecordSubscription(_)
            | Self::OpenObserverSubscription(_) => SemaOperation::Subscribe,
            Self::CloseStateSubscription(_)
            | Self::CloseRecordSubscription(_)
            | Self::CloseObserverSubscription(_) => SemaOperation::Retract,
        }
    }
}

impl ToSemaOutcome for Effect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::RecordAccepted(_) => SemaOutcome::Asserted,
            Self::StateObserved(_)
            | Self::RecordsObserved(_)
            | Self::RecordProvenancesObserved(_)
            | Self::QuestionsObserved(_) => SemaOutcome::Matched,
            Self::StateSubscriptionOpened(_)
            | Self::RecordSubscriptionOpened(_)
            | Self::SubscriptionOpened(_)
            | Self::ObserverSubscriptionOpened(_) => SemaOutcome::Subscribed,
            Self::StateSubscriptionRetracted(_)
            | Self::RecordSubscriptionRetracted(_)
            | Self::SubscriptionRetracted(_) => SemaOutcome::Retracted,
            Self::RequestUnimplemented(_) => SemaOutcome::NoChange,
        }
    }
}

impl From<RecordQuery> for Command {
    fn from(query: RecordQuery) -> Self {
        Self::ReadRecords(RecordObservation { query })
    }
}
