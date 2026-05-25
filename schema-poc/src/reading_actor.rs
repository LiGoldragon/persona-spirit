//! Reading actor (psyche 2026-05-26 — INTERNAL CHANNEL LANGUAGE).
//! Per /346 §5 the reading actor is itself an actor with its own
//! schema. Its FanOutTargets ALWAYS include `(Tap LogSinkSet
//! WriteEntry)` — the auto-tap is declaratively encoded.

use std::sync::{Arc, Mutex};

use crate::spirit_reading_actor::{
    DispatchEnvelope, Identifier, ReadingActorAction, ReadingActorResponse,
    ResponseDispatched,
};

#[derive(Clone, Default)]
pub struct SpiritReadingActor {
    tap_log: Arc<Mutex<Vec<TapEntry>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TapEntry {
    pub destination_identifier: u64,
    pub correlation_identifier: u64,
}

impl SpiritReadingActor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle(&self, action: ReadingActorAction) -> ReadingActorResponse {
        match action {
            ReadingActorAction::DispatchRecorderResponse(envelope) => self.dispatch(envelope),
            ReadingActorAction::DispatchObserverResponse(envelope) => self.dispatch(envelope),
        }
    }

    fn dispatch(&self, envelope: DispatchEnvelope) -> ReadingActorResponse {
        let DispatchEnvelope {
            identifier: Identifier(destination),
            identifier_2: Identifier(correlation),
        } = envelope;
        // The auto-tap fan-out — declaratively encoded in the schema's
        // FanOutTargets as `(Tap LogSinkSet WriteEntry)`. The reading
        // actor mirrors that by writing every dispatch into the
        // in-memory tap log.
        self.tap_log
            .lock()
            .expect("tap log mutex poisoned")
            .push(TapEntry {
                destination_identifier: destination,
                correlation_identifier: correlation,
            });
        ReadingActorResponse::ResponseDispatched(ResponseDispatched(Identifier(destination)))
    }

    pub fn tap_log(&self) -> Vec<TapEntry> {
        self.tap_log
            .lock()
            .expect("tap log mutex poisoned")
            .clone()
    }
}
