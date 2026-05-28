use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_persona_spirit::{
    RecordIdentifier, RecordIdentifierQuery, RecordObservation, RecordSubscription, RecordSummary,
    Reply as WorkingReply,
};
use signal_version_handover::{HandoverMarker, MarkerRequest};

use crate::{
    Result, SpiritStore, StoreLocation,
    store::{StampedEntry, spirit_contract_version},
};

use super::pipeline::PipelineReply;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct RecordStore {
    store: SpiritStore,
}

#[derive(Clone)]
pub struct Arguments {
    pub location: StoreLocation,
}

pub struct CaptureEntry {
    pub entry: StampedEntry,
    pub trace: ActorTrace,
}

pub struct RemoveEntry {
    pub identifier: RecordIdentifier,
    pub trace: ActorTrace,
}

pub struct ObserveRecords {
    pub observation: RecordObservation,
    pub trace: ActorTrace,
}

pub struct ObserveRecordIdentifiers {
    pub query: RecordIdentifierQuery,
    pub trace: ActorTrace,
}

pub struct ObserveTopics {
    pub trace: ActorTrace,
}

pub struct ReadRecordSnapshot {
    pub subscription: RecordSubscription,
    pub trace: ActorTrace,
}

pub struct ReadHandoverMarker {
    pub request: MarkerRequest,
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct RecordSnapshot {
    pub records: Vec<RecordSummary>,
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct HandoverMarkerSnapshot {
    pub marker: HandoverMarker,
    pub trace: ActorTrace,
}

impl RecordStore {
    fn new(store: SpiritStore) -> Self {
        Self { store }
    }

    fn capture_entry(&self, entry: StampedEntry, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_WRITER, TraceAction::MessageReceived);
        let accepted = self.store.assert_entry(entry)?;
        trace.record(TraceNode::SEMA_WRITER, TraceAction::RecordCommitted);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(
            WorkingReply::RecordAccepted(accepted),
            trace,
        ))
    }

    fn remove_entry(
        &self,
        identifier: RecordIdentifier,
        mut trace: ActorTrace,
    ) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_WRITER, TraceAction::MessageReceived);
        let removed = self.store.remove_entry(identifier)?;
        trace.record(TraceNode::SEMA_WRITER, TraceAction::RecordRetracted);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(
            WorkingReply::RecordRemoved(removed),
            trace,
        ))
    }

    fn observe_records(
        &self,
        observation: RecordObservation,
        mut trace: ActorTrace,
    ) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_READER, TraceAction::MessageReceived);
        let reply = self.store.observe_records(observation)?;
        trace.record(TraceNode::SEMA_READER, TraceAction::RecordsRead);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(reply, trace))
    }

    fn observe_record_identifiers(
        &self,
        query: RecordIdentifierQuery,
        mut trace: ActorTrace,
    ) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_READER, TraceAction::MessageReceived);
        let reply = self.store.observe_record_identifiers(query)?;
        trace.record(TraceNode::SEMA_READER, TraceAction::RecordsRead);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(reply, trace))
    }

    fn observe_topics(&self, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_READER, TraceAction::MessageReceived);
        let reply = self.store.observe_topics()?;
        trace.record(TraceNode::SEMA_READER, TraceAction::RecordsRead);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(reply, trace))
    }

    fn read_record_snapshot(
        &self,
        subscription: RecordSubscription,
        mut trace: ActorTrace,
    ) -> Result<RecordSnapshot> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_READER, TraceAction::MessageReceived);
        let records = self
            .store
            .summaries_for_topic(subscription.topic.as_ref())?;
        trace.record(TraceNode::SEMA_READER, TraceAction::RecordsRead);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(RecordSnapshot { records, trace })
    }

    fn read_handover_marker(
        &self,
        request: MarkerRequest,
        mut trace: ActorTrace,
    ) -> Result<HandoverMarkerSnapshot> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_READER, TraceAction::MessageReceived);
        let marker = self
            .store
            .handover_marker(request, spirit_contract_version())?;
        trace.record(TraceNode::SEMA_READER, TraceAction::RecordsRead);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(HandoverMarkerSnapshot { marker, trace })
    }
}

impl Actor for RecordStore {
    type Args = Arguments;
    type Error = crate::Error;

    async fn on_start(arguments: Self::Args, _actor_reference: ActorRef<Self>) -> Result<Self> {
        Ok(Self::new(SpiritStore::open(&arguments.location)?))
    }
}

impl Message<CaptureEntry> for RecordStore {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: CaptureEntry,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.capture_entry(message.entry, message.trace)
    }
}

impl Message<RemoveEntry> for RecordStore {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: RemoveEntry,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.remove_entry(message.identifier, message.trace)
    }
}

impl Message<ObserveRecords> for RecordStore {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: ObserveRecords,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_records(message.observation, message.trace)
    }
}

impl Message<ObserveRecordIdentifiers> for RecordStore {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: ObserveRecordIdentifiers,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_record_identifiers(message.query, message.trace)
    }
}

impl Message<ObserveTopics> for RecordStore {
    type Reply = Result<PipelineReply>;

    async fn handle(
        &mut self,
        message: ObserveTopics,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_topics(message.trace)
    }
}

impl Message<ReadRecordSnapshot> for RecordStore {
    type Reply = Result<RecordSnapshot>;

    async fn handle(
        &mut self,
        message: ReadRecordSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.read_record_snapshot(message.subscription, message.trace)
    }
}

impl Message<ReadHandoverMarker> for RecordStore {
    type Reply = Result<HandoverMarkerSnapshot>;

    async fn handle(
        &mut self,
        message: ReadHandoverMarker,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.read_handover_marker(message.request, message.trace)
    }
}
