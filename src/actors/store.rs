use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_persona_spirit::{Entry, RecordObservation, SpiritReply};

use crate::{Result, SpiritStore, StoreLocation};

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
    pub entry: Entry,
    pub trace: ActorTrace,
}

pub struct ObserveRecords {
    pub observation: RecordObservation,
    pub trace: ActorTrace,
}

impl RecordStore {
    fn new(store: SpiritStore) -> Self {
        Self { store }
    }

    fn capture_entry(&self, entry: Entry, mut trace: ActorTrace) -> Result<PipelineReply> {
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReceived);
        trace.record(TraceNode::SEMA_WRITER, TraceAction::MessageReceived);
        let accepted = self.store.assert_entry(entry)?;
        trace.record(TraceNode::SEMA_WRITER, TraceAction::RecordCommitted);
        trace.record(TraceNode::RECORD_STORE, TraceAction::MessageReplied);
        Ok(PipelineReply::new(
            SpiritReply::RecordAccepted(accepted),
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
