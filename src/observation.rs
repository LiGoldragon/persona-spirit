//! Projection from Spirit-local execution records to universal Sema
//! observation labels.
//!
//! The executable records remain local to this component. `signal-sema`
//! receives only payloadless classification labels.

use signal_persona_spirit::{
    Observation, QuestionsObserved, RecordAccepted, RecordObservation, RecordProvenancesObserved,
    RecordQuery, RecordSubscription, RecordSubscriptionToken, RecordsObserved,
    RequestUnimplemented, SpiritObserverFilter, SpiritObserverSubscriptionOpened,
    SpiritObserverSubscriptionToken, SpiritReply, SpiritRequest, StateObserved,
    StateSubscriptionToken, Statement, Subscription, SubscriptionOpened, SubscriptionRetracted,
    SubscriptionToken,
};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    ClassifyStatement(Statement),
    AssertEntry(signal_persona_spirit::Entry),
    ReadRecords(RecordObservation),
    ReadState,
    ReadQuestions,
    OpenStateSubscription,
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
    SubscriptionOpened(SubscriptionOpened),
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
            SpiritRequest::Observe(Observation::State) => Some(Self::ReadState),
            SpiritRequest::Observe(Observation::Questions) => Some(Self::ReadQuestions),
            SpiritRequest::Watch(Subscription::State) => Some(Self::OpenStateSubscription),
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
            SpiritReply::SubscriptionOpened(payload) => Self::SubscriptionOpened(payload),
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

    pub fn into_reply(self) -> SpiritReply {
        match self {
            Self::RecordAccepted(payload) => SpiritReply::RecordAccepted(payload),
            Self::StateObserved(payload) => SpiritReply::StateObserved(payload),
            Self::RecordsObserved(payload) => SpiritReply::RecordsObserved(payload),
            Self::RecordProvenancesObserved(payload) => {
                SpiritReply::RecordProvenancesObserved(payload)
            }
            Self::QuestionsObserved(payload) => SpiritReply::QuestionsObserved(payload),
            Self::SubscriptionOpened(payload) => SpiritReply::SubscriptionOpened(payload),
            Self::SubscriptionRetracted(payload) => SpiritReply::SubscriptionRetracted(payload),
            Self::ObserverSubscriptionOpened(payload) => {
                SpiritReply::ObserverSubscriptionOpened(payload)
            }
            Self::RequestUnimplemented(payload) => SpiritReply::RequestUnimplemented(payload),
        }
    }
}

impl ToSemaOperation for Command {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::ClassifyStatement(_) | Self::AssertEntry(_) => SemaOperation::Assert,
            Self::ReadRecords(_) | Self::ReadState | Self::ReadQuestions => SemaOperation::Match,
            Self::OpenStateSubscription
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
            Self::SubscriptionOpened(_) | Self::ObserverSubscriptionOpened(_) => {
                SemaOutcome::Subscribed
            }
            Self::SubscriptionRetracted(_) => SemaOutcome::Retracted,
            Self::RequestUnimplemented(_) => SemaOutcome::NoChange,
        }
    }
}

impl From<RecordQuery> for Command {
    fn from(query: RecordQuery) -> Self {
        Self::ReadRecords(RecordObservation { query })
    }
}
