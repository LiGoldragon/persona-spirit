//! Observer actor (psyche 2026-05-26 + intent records 709, 710 —
//! INTERNAL CHANNEL LANGUAGE, language 3 of 3).
//!
//! The observer serves the v0.3 `Observe Topics` + `Observe Records`
//! wire operations. For the POC the observer returns empty results
//! by default; production-emulating tests assert the variant shape
//! matches v0.3.

use std::sync::Arc;

use crate::spirit_observer::{
    ObserveRecordsRequest, ObserveTopicsRequest, ObserverAction, ObserverResponse,
    RecordProvenancesObserved, RecordsObserved, TopicsObserved,
};
use crate::storage::SpiritStorageHandle;

#[derive(Clone)]
pub struct SpiritObserver {
    _storage: Arc<SpiritStorageHandle>,
}

impl SpiritObserver {
    pub fn new(storage: Arc<SpiritStorageHandle>) -> Self {
        Self { _storage: storage }
    }

    pub fn handle(&self, action: ObserverAction) -> ObserverResponse {
        match action {
            ObserverAction::ObserveTopics(request) => self.observe_topics(request),
            ObserverAction::ObserveRecordsDescriptionOnly(request) => {
                self.observe_records_description(request)
            }
            ObserverAction::ObserveRecordsWithProvenance(request) => {
                self.observe_records_provenance(request)
            }
        }
    }

    fn observe_topics(&self, _request: ObserveTopicsRequest) -> ObserverResponse {
        ObserverResponse::TopicsObserved(TopicsObserved(Vec::new()))
    }

    fn observe_records_description(
        &self,
        _request: ObserveRecordsRequest,
    ) -> ObserverResponse {
        ObserverResponse::RecordsObserved(RecordsObserved(Vec::new()))
    }

    fn observe_records_provenance(
        &self,
        _request: ObserveRecordsRequest,
    ) -> ObserverResponse {
        ObserverResponse::RecordProvenancesObserved(RecordProvenancesObserved(Vec::new()))
    }
}
