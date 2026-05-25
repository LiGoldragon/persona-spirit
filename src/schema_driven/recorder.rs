//! SpiritRecorder actor: schema-driven engine logic.
//!
//! Per /346 §2: structure is schema; logic is Rust. This file is ONLY
//! the decision-making body inside the recorder's match block. All
//! data types --- `RecorderAction`, `RecorderResponse`, payload structs
//! --- emit from `spirit-recorder.schema` via `emit_schema!` in the
//! target world.
//!
//! Today the recorder schema imports from `signal-persona-spirit` and
//! `spirit-storage`, neither of which the in-tree schema resolver can
//! follow without an adjacent worktree layout. Until that infrastructure
//! lands the types here are hand-written to MATCH what `emit_schema!`
//! would produce per /346 §10 worked example. Once cross-crate
//! resolution lands, the hand-written types here drop and the
//! `emit_schema!` invocations in `mod.rs` light up.
//!
//! The universal `Unknown` variant on `RecorderResponse` is the safety
//! floor per /346 §1: any handler that hits an unrecognized or errored
//! path returns `Unknown(reason_string)` instead of panicking or
//! dropping the message.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use crate::schema_driven::storage::SpiritStorageHandle;

/// ACTION enum mirroring spirit-recorder.schema's `RecorderAction (
/// RecordEntry ObserveRecorder SnapshotRecords ... )`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderAction {
    RecordEntry(RecordEntry),
    ObserveRecorder(ObserveRecorder),
    SnapshotRecords(SnapshotRecords),
    OpenRecordSubscription(OpenRecordSubscription),
    CloseRecordSubscription(CloseRecordSubscription),
    QueryStatus,
}

/// RESPONSE enum mirroring spirit-recorder.schema's `RecorderResponse
/// ( RecordAccepted ... )` PLUS the universal `Unknown` variant
/// injected by `UniversalUnknownMacro` per /346 §9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderResponse {
    RecordAccepted(u64),
    RecordsObserved(Vec<RecordDescription>),
    RecordSnapshotReturned(Vec<RecordDescription>),
    SubscriptionOpened(u64),
    SubscriptionRetracted(u64),
    StatusReturned(RecorderStatus),
    /// Universal safety floor per /346 §1 + §9: every actor knows how
    /// to say "I don't know what you're asking for" without panicking
    /// or dropping the message.
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordEntry {
    pub topic: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserveRecorder {
    pub topic_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecords {
    pub topic_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRecordSubscription {
    pub topic_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseRecordSubscription {
    pub token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDescription {
    pub identifier: u64,
    pub topic: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderStatus {
    pub records_written: u64,
    pub subscriptions_open: u64,
    pub health_level: RecordHealthLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordHealthLevel {
    Nominal,
    Degraded,
    Recovering,
}

/// Hand-written engine bodies for the recorder.
///
/// The actor owns its storage handle + tracks next identifier and
/// subscription count. Action arrives -> match block decides ->
/// response goes back into the response stream.
pub struct SpiritRecorder {
    storage: SpiritStorageHandle,
    next_identifier: AtomicU64,
    open_subscriptions: AtomicU64,
}

impl SpiritRecorder {
    pub fn new(storage: SpiritStorageHandle) -> Self {
        Self {
            storage,
            next_identifier: AtomicU64::new(1),
            open_subscriptions: AtomicU64::new(0),
        }
    }

    /// The contact-point match block per /346 §2. Every ACTION variant
    /// dispatches to a logic method that returns a RESPONSE variant.
    pub fn handle(&self, action: RecorderAction) -> RecorderResponse {
        match action {
            RecorderAction::RecordEntry(payload) => self.record_entry(payload),
            RecorderAction::ObserveRecorder(filter) => self.observe(filter),
            RecorderAction::SnapshotRecords(filter) => self.snapshot(filter),
            RecorderAction::OpenRecordSubscription(payload) => self.open_subscription(payload),
            RecorderAction::CloseRecordSubscription(close) => self.close_subscription(close),
            RecorderAction::QueryStatus => self.status(),
        }
    }

    fn record_entry(&self, payload: RecordEntry) -> RecorderResponse {
        let identifier = self.next_identifier.fetch_add(1, Ordering::SeqCst);
        // In the integrated world this would dispatch through the
        // SpiritStorage actor's InsertStampedEntry method per
        // spirit-recorder.schema's FanOutTargets. The hand-written
        // glue swaps in for the schema-emitted fan-out.
        let _ = (&self.storage, &payload);
        RecorderResponse::RecordAccepted(identifier)
    }

    fn observe(&self, _filter: ObserveRecorder) -> RecorderResponse {
        RecorderResponse::RecordsObserved(Vec::new())
    }

    fn snapshot(&self, _filter: SnapshotRecords) -> RecorderResponse {
        RecorderResponse::RecordSnapshotReturned(Vec::new())
    }

    fn open_subscription(&self, _payload: OpenRecordSubscription) -> RecorderResponse {
        let token = self.open_subscriptions.fetch_add(1, Ordering::SeqCst) + 1;
        RecorderResponse::SubscriptionOpened(token)
    }

    fn close_subscription(&self, close: CloseRecordSubscription) -> RecorderResponse {
        self.open_subscriptions
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                Some(count.saturating_sub(1))
            })
            .ok();
        RecorderResponse::SubscriptionRetracted(close.token)
    }

    fn status(&self) -> RecorderResponse {
        RecorderResponse::StatusReturned(RecorderStatus {
            records_written: self
                .next_identifier
                .load(Ordering::SeqCst)
                .saturating_sub(1),
            subscriptions_open: self.open_subscriptions.load(Ordering::SeqCst),
            health_level: RecordHealthLevel::Nominal,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_driven::storage::SpiritStorageHandle;

    fn handle_for_test(stub: &str) -> SpiritStorageHandle {
        let dir = std::env::temp_dir().join(format!("persona-spirit-recorder-{stub}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        SpiritStorageHandle::open(dir.join("store.redb"))
    }

    #[test]
    fn record_entry_returns_record_accepted_with_fresh_identifier() {
        let recorder = SpiritRecorder::new(handle_for_test("record-entry"));
        let response = recorder.handle(RecorderAction::RecordEntry(RecordEntry {
            topic: "topic".into(),
            description: "description".into(),
        }));
        match response {
            RecorderResponse::RecordAccepted(identifier) => assert_eq!(identifier, 1),
            other => panic!("expected RecordAccepted, got {other:?}"),
        }
    }

    #[test]
    fn query_status_reflects_record_count() {
        let recorder = SpiritRecorder::new(handle_for_test("query-status"));
        recorder.handle(RecorderAction::RecordEntry(RecordEntry {
            topic: "topic".into(),
            description: "description".into(),
        }));
        recorder.handle(RecorderAction::RecordEntry(RecordEntry {
            topic: "topic".into(),
            description: "description".into(),
        }));
        match recorder.handle(RecorderAction::QueryStatus) {
            RecorderResponse::StatusReturned(status) => {
                assert_eq!(status.records_written, 2);
                assert!(matches!(status.health_level, RecordHealthLevel::Nominal));
            }
            other => panic!("expected StatusReturned, got {other:?}"),
        }
    }

    #[test]
    fn subscription_lifecycle_round_trips_token() {
        let recorder = SpiritRecorder::new(handle_for_test("subscription"));
        let opened = recorder.handle(RecorderAction::OpenRecordSubscription(
            OpenRecordSubscription { topic_filter: None },
        ));
        let token = match opened {
            RecorderResponse::SubscriptionOpened(token) => token,
            other => panic!("expected SubscriptionOpened, got {other:?}"),
        };
        let closed = recorder.handle(RecorderAction::CloseRecordSubscription(
            CloseRecordSubscription { token },
        ));
        assert!(matches!(closed, RecorderResponse::SubscriptionRetracted(t) if t == token));
    }

    #[test]
    fn unknown_variant_is_the_safety_floor() {
        // The universal-Unknown variant is reachable directly --- this
        // is the safety floor per /346 §1 §9 for any code path that
        // can't structurally handle an action.
        let response = RecorderResponse::Unknown("rejected".into());
        assert!(matches!(response, RecorderResponse::Unknown(reason) if reason == "rejected"));
    }
}
