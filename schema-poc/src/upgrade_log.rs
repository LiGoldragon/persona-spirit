//! Upgrade-log actor (psyche 2026-05-26 — INTERNAL CHANNEL LANGUAGE).
//! Tracks per-migration boundary outcomes.

use std::sync::{Arc, Mutex};

use crate::spirit_upgrade_log::{
    AppendUpgradeEntryRequest, Identifier, QueryUpgradeLogRequest, UpgradeEntryAppended,
    UpgradeLogAction, UpgradeLogEntry, UpgradeLogResponse, UpgradeLogReturned,
};

#[derive(Clone, Default)]
pub struct SpiritUpgradeLog {
    entries: Arc<Mutex<Vec<UpgradeLogEntry>>>,
    next_identifier: Arc<Mutex<u64>>,
}

impl SpiritUpgradeLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle(&self, action: UpgradeLogAction) -> UpgradeLogResponse {
        match action {
            UpgradeLogAction::AppendUpgradeEntry(request) => self.append(request),
            UpgradeLogAction::QueryUpgradeLog(request) => self.query(request),
        }
    }

    fn append(&self, _request: AppendUpgradeEntryRequest) -> UpgradeLogResponse {
        // The POC ignores the request payload here — the entry is
        // built deterministically. Real persistence + provenance
        // stamping lives in the daemon's storage handle.
        let mut identifier_guard = self
            .next_identifier
            .lock()
            .expect("upgrade-log identifier mutex poisoned");
        let identifier_value = *identifier_guard;
        *identifier_guard += 1;
        UpgradeLogResponse::UpgradeEntryAppended(UpgradeEntryAppended(Identifier(
            identifier_value,
        )))
    }

    fn query(&self, _request: QueryUpgradeLogRequest) -> UpgradeLogResponse {
        UpgradeLogResponse::UpgradeLogReturned(UpgradeLogReturned(
            self.entries
                .lock()
                .expect("upgrade-log entries mutex poisoned")
                .clone(),
        ))
    }
}
