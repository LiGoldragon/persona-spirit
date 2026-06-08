//! Projection from Spirit-local execution records to universal Sema
//! observation labels.
//!
//! The executable records remain local to this component. `signal-sema`
//! receives only payloadless classification labels.

use signal_sema::{SemaObservation, SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};
use signal_spirit::{
    CertaintyChange, CertaintyChanged, EffectOutcome, Observation, ObserverFilter,
    ObserverSubscriptionOpened, ObserverSubscriptionToken, Operation as WorkingOperation,
    OperationKind, PrivacySelection, QuestionsObserved, RecordAccepted, RecordChange,
    RecordIdentifier, RecordIdentifierQuery, RecordMutationApplied, RecordObservation,
    RecordProvenancesObserved, RecordQuery, RecordRemoved, RecordSubscription,
    RecordSubscriptionToken, RecordsObserved, RemovalCandidateCollection,
    RemovalCandidatesCollected, Reply as WorkingReply, RequestUnimplemented, StateObserved,
    StateSubscriptionToken, Statement, Subscription, SubscriptionOpened, SubscriptionRetracted,
    SubscriptionToken, TopicsObserved,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    ClassifyStatement(Statement),
    AssertEntry(signal_spirit::Entry),
    RemoveRecord(RecordIdentifier),
    ChangeCertainty(CertaintyChange),
    ChangeRecord(RecordChange),
    CollectRemovalCandidates(RemovalCandidateCollection),
    ReadRecords(RecordObservation),
    ReadRecordIdentifiers(RecordIdentifierObservation),
    ReadTopics,
    ReadState,
    ReadQuestions,
    OpenStateSubscription,
    OpenRecordSubscription(RecordSubscriptionObservation),
    CloseStateSubscription(StateSubscriptionToken),
    CloseRecordSubscription(RecordSubscriptionToken),
    OpenObserverSubscription(ObserverFilter),
    CloseObserverSubscription(ObserverSubscriptionToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    RecordAccepted(RecordAccepted),
    RecordRemoved(RecordRemoved),
    CertaintyChanged(CertaintyChanged),
    RecordMutationApplied(RecordMutationApplied),
    RemovalCandidatesCollected(RemovalCandidatesCollected),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordIdentifierObservation {
    pub query: RecordIdentifierQuery,
    pub privacy_selection: PrivacySelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSubscriptionObservation {
    pub subscription: RecordSubscription,
    pub privacy_selection: PrivacySelection,
}

impl RecordIdentifierObservation {
    pub fn public(query: RecordIdentifierQuery) -> Self {
        Self {
            query,
            privacy_selection: PrivacySelection::default_observation_privacy(),
        }
    }

    pub fn private(query: signal_spirit::PrivacyScopedRecordIdentifierQuery) -> Self {
        Self {
            query: query.record_identifier_query,
            privacy_selection: query.privacy_selection,
        }
    }
}

impl RecordSubscriptionObservation {
    pub fn public(subscription: RecordSubscription) -> Self {
        Self {
            subscription,
            privacy_selection: PrivacySelection::default_observation_privacy(),
        }
    }

    pub fn private(subscription: signal_spirit::PrivacyScopedRecordSubscription) -> Self {
        Self {
            subscription: subscription.record_subscription,
            privacy_selection: subscription.privacy_selection,
        }
    }
}

impl Command {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::ClassifyStatement(_) => OperationKind::State,
            Self::AssertEntry(_) => OperationKind::Record,
            Self::RemoveRecord(_) => OperationKind::Remove,
            Self::ChangeCertainty(_) => OperationKind::ChangeCertainty,
            Self::ChangeRecord(_) => OperationKind::ChangeRecord,
            Self::CollectRemovalCandidates(_) => OperationKind::CollectRemovalCandidates,
            Self::ReadRecords(_)
            | Self::ReadRecordIdentifiers(_)
            | Self::ReadTopics
            | Self::ReadState
            | Self::ReadQuestions => OperationKind::Observe,
            Self::OpenStateSubscription | Self::OpenRecordSubscription(_) => OperationKind::Watch,
            Self::CloseStateSubscription(_) | Self::CloseRecordSubscription(_) => {
                OperationKind::Unwatch
            }
            Self::OpenObserverSubscription(_) => OperationKind::Tap,
            Self::CloseObserverSubscription(_) => OperationKind::Untap,
        }
    }

    pub fn from_request(request: WorkingOperation) -> Option<Self> {
        match request {
            WorkingOperation::State(statement) => Some(Self::ClassifyStatement(statement)),
            WorkingOperation::Record(entry) => Some(Self::AssertEntry(entry)),
            WorkingOperation::Remove(identifier) => Some(Self::RemoveRecord(identifier)),
            WorkingOperation::ChangeCertainty(change) => Some(Self::ChangeCertainty(change)),
            WorkingOperation::ChangeRecord(change) => Some(Self::ChangeRecord(change)),
            WorkingOperation::CollectRemovalCandidates(collection) => {
                Some(Self::CollectRemovalCandidates(collection))
            }
            WorkingOperation::Observe(Observation::Records(query)) => {
                Some(Self::ReadRecords(RecordObservation {
                    query: query.into_record_query(),
                }))
            }
            WorkingOperation::Observe(Observation::PrivateRecords(query)) => {
                Some(Self::ReadRecords(RecordObservation {
                    query: query.into_record_query(),
                }))
            }
            WorkingOperation::Observe(Observation::RecordIdentifiers(query)) => Some(
                Self::ReadRecordIdentifiers(RecordIdentifierObservation::public(query)),
            ),
            WorkingOperation::Observe(Observation::PrivateRecordIdentifiers(query)) => Some(
                Self::ReadRecordIdentifiers(RecordIdentifierObservation::private(query)),
            ),
            WorkingOperation::Observe(Observation::Topics) => Some(Self::ReadTopics),
            WorkingOperation::Observe(Observation::State) => Some(Self::ReadState),
            WorkingOperation::Observe(Observation::Questions) => Some(Self::ReadQuestions),
            WorkingOperation::Watch(Subscription::State) => Some(Self::OpenStateSubscription),
            WorkingOperation::Watch(Subscription::Records(subscription)) => Some(
                Self::OpenRecordSubscription(RecordSubscriptionObservation::public(subscription)),
            ),
            WorkingOperation::Watch(Subscription::PrivateRecords(subscription)) => Some(
                Self::OpenRecordSubscription(RecordSubscriptionObservation::private(subscription)),
            ),
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
}

impl Effect {
    pub fn outcome(&self) -> EffectOutcome {
        match self {
            Self::RecordAccepted(_) => EffectOutcome::RecordCaptured,
            Self::RecordRemoved(_) => EffectOutcome::RecordRemoved,
            Self::CertaintyChanged(_) => EffectOutcome::CertaintyChanged,
            Self::RecordMutationApplied(_) => EffectOutcome::RecordChanged,
            Self::RemovalCandidatesCollected(_) => EffectOutcome::RemovalCandidatesCollected,
            Self::StateObserved(_)
            | Self::RecordsObserved(_)
            | Self::RecordProvenancesObserved(_)
            | Self::TopicsObserved(_)
            | Self::QuestionsObserved(_) => EffectOutcome::Observed,
            Self::SubscriptionOpened(_) | Self::ObserverSubscriptionOpened(_) => {
                EffectOutcome::StreamOpened
            }
            Self::SubscriptionRetracted(_) => EffectOutcome::StreamClosed,
            Self::RequestUnimplemented(_) => EffectOutcome::NoChange,
        }
    }

    pub fn from_reply(reply: WorkingReply) -> Self {
        match reply {
            WorkingReply::RecordAccepted(payload) => Self::RecordAccepted(payload),
            WorkingReply::RecordRemoved(payload) => Self::RecordRemoved(payload),
            WorkingReply::CertaintyChanged(payload) => Self::CertaintyChanged(payload),
            WorkingReply::RecordMutationApplied(payload) => Self::RecordMutationApplied(payload),
            WorkingReply::RemovalCandidatesCollected(payload) => {
                Self::RemovalCandidatesCollected(payload)
            }
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
            Self::RecordRemoved(payload) => WorkingReply::RecordRemoved(payload),
            Self::CertaintyChanged(payload) => WorkingReply::CertaintyChanged(payload),
            Self::RecordMutationApplied(payload) => WorkingReply::RecordMutationApplied(payload),
            Self::RemovalCandidatesCollected(payload) => {
                WorkingReply::RemovalCandidatesCollected(payload)
            }
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
}

impl ToSemaOperation for Command {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::ClassifyStatement(_) | Self::AssertEntry(_) => SemaOperation::Assert,
            Self::ChangeCertainty(_) | Self::ChangeRecord(_) => SemaOperation::Mutate,
            Self::RemoveRecord(_) | Self::CollectRemovalCandidates(_) => SemaOperation::Retract,
            Self::ReadRecords(_)
            | Self::ReadRecordIdentifiers(_)
            | Self::ReadTopics
            | Self::ReadState
            | Self::ReadQuestions => SemaOperation::Match,
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
            Self::CertaintyChanged(_) | Self::RecordMutationApplied(_) => SemaOutcome::Mutated,
            Self::RecordRemoved(_) | Self::RemovalCandidatesCollected(_) => SemaOutcome::Retracted,
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
