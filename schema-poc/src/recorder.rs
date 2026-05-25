//! Recorder actor (psyche 2026-05-26 + intent records 709, 710 —
//! INTERNAL CHANNEL LANGUAGE, language 3 of 3).
//!
//! The recorder serves the v0.3 `Record` wire operation. It mints a
//! fresh identifier from the storage handle and replies with
//! `RecordAccepted(identifier)`.

use std::sync::Arc;

use crate::spirit_recorder::{
    Identifier, RecordAccepted, RecorderAction, RecorderResponse, RecorderStatus,
    RecorderStatusReturned,
};
use crate::storage::SpiritStorageHandle;

/// In-memory recorder state. Holds the storage handle (for identifier
/// minting) plus a counter of records observed for the status query.
#[derive(Clone)]
pub struct SpiritRecorder {
    storage: Arc<SpiritStorageHandle>,
    records_observed: Arc<std::sync::Mutex<u64>>,
}

impl SpiritRecorder {
    pub fn new(storage: Arc<SpiritStorageHandle>) -> Self {
        Self {
            storage,
            records_observed: Arc::new(std::sync::Mutex::new(0)),
        }
    }

    /// The contact-point match block per /346 §2. Structure is the
    /// schema; logic is Rust.
    pub fn handle(&self, action: RecorderAction) -> RecorderResponse {
        match action {
            RecorderAction::RecordEntry(_request) => self.record_entry(),
            RecorderAction::QueryRecorderStatus(_request) => self.query_status(),
        }
    }

    fn record_entry(&self) -> RecorderResponse {
        let identifier = self.storage.mint_identifier();
        {
            let mut guard = self
                .records_observed
                .lock()
                .expect("records_observed mutex poisoned");
            *guard += 1;
        }
        RecorderResponse::RecordAccepted(RecordAccepted(Identifier(identifier)))
    }

    fn query_status(&self) -> RecorderResponse {
        let observed = *self
            .records_observed
            .lock()
            .expect("records_observed mutex poisoned");
        RecorderResponse::RecorderStatusReturned(RecorderStatusReturned(RecorderStatus {
            u64: observed,
            u64_2: 0,
        }))
    }
}
