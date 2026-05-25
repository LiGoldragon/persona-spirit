//! Projection from Spirit-local execution records to universal Sema
//! observation labels.
//!
//! The executable records remain local to this component. `signal-sema`
//! receives only payloadless classification labels.

use signal_persona_spirit::{
    Observation, ObservationMode, ObserverFilter, ObserverSubscriptionOpened,
    ObserverSubscriptionToken, Operation as WorkingOperation, QuestionsObserved, RecordAccepted,
    RecordObservation, RecordProvenancesObserved, RecordQuery, RecordSubscription,
    RecordSubscriptionToken, RecordsObserved, Reply as WorkingReply, RequestUnimplemented,
    StateObserved, StateSubscriptionToken, Statement, Subscription, SubscriptionOpened,
    SubscriptionRetracted, SubscriptionToken, TopicsObserved,
};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    ClassifyStatement(Statement),
    AssertEntry(signal_persona_spirit::Entry),
    ReadRecords(RecordObservation),
    ReadTopics,
    ReadState,
    ReadQuestions,
    OpenStateSubscription,
    OpenRecordSubscription(RecordSubscription),
    CloseStateSubscription(StateSubscriptionToken),
    CloseRecordSubscription(RecordSubscriptionToken),
    OpenObserverSubscription(ObserverFilter),
    CloseObserverSubscription(ObserverSubscriptionToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    RecordAccepted(RecordAccepted),
    StateObserved(StateObserved),
    RecordsObserved(RecordsObserved),
    RecordProvenancesObserved(RecordProvenancesObserved),
    TopicsObserved(TopicsObserved),
    QuestionsObserved(QuestionsObserved),
    SubscriptionOpened(SubscriptionOpened),
    SubscriptionRetracted(SubscriptionRetracted),
    ObserverSubscriptionOpened(ObserverSubscriptionOpened),
    RequestUnimplemented(RequestUnimplemented),
}

impl Command {
    pub fn from_request(request: WorkingOperation) -> Option<Self> {
        match request {
            WorkingOperation::State(statement) => Some(Self::ClassifyStatement(statement)),
            WorkingOperation::Record(entry) => Some(Self::AssertEntry(entry)),
            WorkingOperation::Observe(Observation::Records(query)) => {
                Some(Self::ReadRecords(RecordObservation { query }))
            }
            WorkingOperation::Observe(Observation::Topics) => Some(Self::ReadTopics),
            WorkingOperation::Observe(Observation::State) => Some(Self::ReadState),
            WorkingOperation::Observe(Observation::Questions) => Some(Self::ReadQuestions),
            WorkingOperation::Watch(Subscription::State) => Some(Self::OpenStateSubscription),
            WorkingOperation::Watch(Subscription::Records(subscription)) => {
                Some(Self::OpenRecordSubscription(subscription))
            }
            WorkingOperation::Unwatch(SubscriptionToken::State(token)) => {
                Some(Self::CloseStateSubscription(token))
            }
            WorkingOperation::Unwatch(SubscriptionToken::Records(token)) => {
                Some(Self::CloseRecordSubscription(token))
            }
            WorkingOperation::Tap(filter) => Some(Self::OpenObserverSubscription(filter)),
            WorkingOperation::Untap(token) => Some(Self::CloseObserverSubscription(token)),
        }
    }

    pub fn schema_action(&self) -> &'static str {
        match self {
            Self::ClassifyStatement(_) => "ClassifyStatement",
            Self::AssertEntry(_) => "AssertEntry",
            Self::ReadRecords(observation) => match observation.query.mode {
                ObservationMode::DescriptionOnly => "ReadRecordDescriptions",
                ObservationMode::WithProvenance => "ReadRecordProvenances",
            },
            Self::ReadTopics => "ReadTopics",
            Self::ReadState => "ReadState",
            Self::ReadQuestions => "ReadQuestions",
            Self::OpenStateSubscription => "OpenStateSubscription",
            Self::OpenRecordSubscription(_) => "OpenRecordSubscription",
            Self::CloseStateSubscription(_) => "CloseStateSubscription",
            Self::CloseRecordSubscription(_) => "CloseRecordSubscription",
            Self::OpenObserverSubscription(_) => "OpenObserverSubscription",
            Self::CloseObserverSubscription(_) => "CloseObserverSubscription",
        }
    }

    pub fn schema_declared_effect(&self) -> Option<&'static str> {
        crate::spirit_runtime::AuthoredEffectTable::effect_for_action(self.schema_action())
    }
}

impl Effect {
    pub fn from_reply(reply: WorkingReply) -> Self {
        match reply {
            WorkingReply::RecordAccepted(payload) => Self::RecordAccepted(payload),
            WorkingReply::StateObserved(payload) => Self::StateObserved(payload),
            WorkingReply::RecordsObserved(payload) => Self::RecordsObserved(payload),
            WorkingReply::RecordProvenancesObserved(payload) => {
                Self::RecordProvenancesObserved(payload)
            }
            WorkingReply::TopicsObserved(payload) => Self::TopicsObserved(payload),
            WorkingReply::QuestionsObserved(payload) => Self::QuestionsObserved(payload),
            WorkingReply::SubscriptionOpened(payload) => Self::SubscriptionOpened(payload),
            WorkingReply::SubscriptionRetracted(payload) => Self::SubscriptionRetracted(payload),
            WorkingReply::ObserverSubscriptionOpened(payload) => {
                Self::ObserverSubscriptionOpened(payload)
            }
            WorkingReply::RequestUnimplemented(payload) => Self::RequestUnimplemented(payload),
        }
    }

    pub fn sema_observation_for(&self, command: &Command) -> SemaObservation {
        SemaObservation::from_projection(command, self)
    }

    pub fn into_reply(self) -> WorkingReply {
        match self {
            Self::RecordAccepted(payload) => WorkingReply::RecordAccepted(payload),
            Self::StateObserved(payload) => WorkingReply::StateObserved(payload),
            Self::RecordsObserved(payload) => WorkingReply::RecordsObserved(payload),
            Self::RecordProvenancesObserved(payload) => {
                WorkingReply::RecordProvenancesObserved(payload)
            }
            Self::TopicsObserved(payload) => WorkingReply::TopicsObserved(payload),
            Self::QuestionsObserved(payload) => WorkingReply::QuestionsObserved(payload),
            Self::SubscriptionOpened(payload) => WorkingReply::SubscriptionOpened(payload),
            Self::SubscriptionRetracted(payload) => WorkingReply::SubscriptionRetracted(payload),
            Self::ObserverSubscriptionOpened(payload) => {
                WorkingReply::ObserverSubscriptionOpened(payload)
            }
            Self::RequestUnimplemented(payload) => WorkingReply::RequestUnimplemented(payload),
        }
    }

    pub fn schema_effect(&self) -> &'static str {
        match self {
            Self::RecordAccepted(_) => "RecordAccepted",
            Self::StateObserved(_) => "StateObserved",
            Self::RecordsObserved(_) => "RecordsObserved",
            Self::RecordProvenancesObserved(_) => "RecordProvenancesObserved",
            Self::TopicsObserved(_) => "TopicsObserved",
            Self::QuestionsObserved(_) => "QuestionsObserved",
            Self::SubscriptionOpened(_) => "SubscriptionOpened",
            Self::SubscriptionRetracted(_) => "SubscriptionRetracted",
            Self::ObserverSubscriptionOpened(_) => "ObserverSubscriptionOpened",
            Self::RequestUnimplemented(_) => "RequestUnimplemented",
        }
    }

    pub fn schema_declared_fan_out(&self) -> Option<crate::spirit_runtime::AuthoredFanOut> {
        crate::spirit_runtime::AuthoredEffectTable::fan_out_for_effect(self.schema_effect())
    }
}

impl ToSemaOperation for Command {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::ClassifyStatement(_) | Self::AssertEntry(_) => SemaOperation::Assert,
            Self::ReadRecords(_) | Self::ReadTopics | Self::ReadState | Self::ReadQuestions => {
                SemaOperation::Match
            }
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
            | Self::TopicsObserved(_)
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
