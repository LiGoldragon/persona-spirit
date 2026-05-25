//! POC daemon glue (psyche 2026-05-26 + intent records 709, 710).
//!
//! Routes v0.3-shape WIRE operations from `signal_persona_spirit` to
//! the internal-channel ACTIONS the actors handle, then projects the
//! actor RESPONSES back to v0.3-shape WIRE replies. The mapping
//! follows the schemas' EffectTable + FanOutTargets, but the POC
//! implements it as a single match block here (rather than a full
//! reading-actor dispatcher) to keep the production-emulating tests
//! straightforward.
//!
//! The wire types come from the schema-driven module on
//! `signal_persona_spirit` (`spirit::*`). For runtime construction
//! the POC uses the LEGACY crate-root types since they carry the
//! `new(...)` constructors + `NotaCodec` derives — these are
//! byte-identical to the schema-driven types per the rkyv-one-format
//! discipline.

use std::sync::Arc;

use signal_persona_spirit as wire;
use signal_sema::Magnitude;
use wire::{Observation, Operation, Reply, Subscription, SubscriptionToken};

use crate::observer::SpiritObserver;
use crate::recorder::SpiritRecorder;
use crate::spirit_observer::{
    ObserveRecordsRequest, ObserveTopicsRequest, ObserverAction, ObserverResponse,
};
use crate::spirit_recorder::{
    Entry as RecorderEntry, Identifier, RecordEntryRequest, RecorderAction, RecorderResponse,
};
use crate::storage::{SpiritStorageHandle, StorageLocation};

/// The POC daemon. Constructed once at startup; one method per wire
/// operation; each method maps the operation to the internal-channel
/// action of the right actor, dispatches, and maps the response back
/// to the wire `Reply`.
pub struct PocDaemon {
    pub storage: Arc<SpiritStorageHandle>,
    pub recorder: SpiritRecorder,
    pub observer: SpiritObserver,
}

impl PocDaemon {
    /// Open the daemon with fresh in-memory storage.
    pub fn open_fresh() -> Self {
        let storage = Arc::new(
            SpiritStorageHandle::open(StorageLocation::InMemory)
                .expect("fresh-DB open cannot fail"),
        );
        let recorder = SpiritRecorder::new(Arc::clone(&storage));
        let observer = SpiritObserver::new(Arc::clone(&storage));
        Self {
            storage,
            recorder,
            observer,
        }
    }

    /// Single-entry dispatch — accepts a v0.3 wire Operation and
    /// returns a v0.3 wire Reply. This is the daemon's external
    /// contact point. The match is closed; Rust enforces
    /// exhaustiveness because the Operation enum is closed.
    pub fn dispatch(&self, operation: Operation) -> Reply {
        match operation {
            Operation::Record(entry) => self.record(entry),
            Operation::State(_statement) => self.state_unimplemented(),
            Operation::Observe(observation) => self.observe(observation),
            Operation::Watch(subscription) => self.watch(subscription),
            Operation::Unwatch(token) => self.unwatch(token),
            // Tap / Untap are placeholder operations in v0.3 per
            // /skills/spirit-cli.md §"Subscribe / unsubscribe". The
            // POC mirrors that they remain unimplemented.
            Operation::Tap(_) => Self::unimplemented_reply(),
            Operation::Untap(_) => Self::unimplemented_reply(),
        }
    }

    fn record(&self, entry: wire::Entry) -> Reply {
        let recorder_entry = RecorderEntry {
            topics: (&entry.topics).into(),
            kind: entry.kind.into(),
            description: crate::spirit_recorder::Description(entry.description.as_str().to_string()),
            certainty: entry.certainty.into(),
        };
        let action = RecorderAction::RecordEntry(RecordEntryRequest(recorder_entry));
        match self.recorder.handle(action) {
            RecorderResponse::RecordAccepted(payload) => {
                let Identifier(value) = payload.0;
                Reply::RecordAccepted(wire::RecordAccepted::new(wire::RecordIdentifier::new(
                    value,
                )))
            }
            other => Reply::RequestUnimplemented(wire::RequestUnimplemented {
                reason: wire::UnimplementedReason::NotBuiltYet,
            })
            .also_log(other),
        }
    }

    fn observe(&self, observation: Observation) -> Reply {
        match observation {
            Observation::Topics => {
                let action = ObserverAction::ObserveTopics(ObserveTopicsRequest(0));
                match self.observer.handle(action) {
                    ObserverResponse::TopicsObserved(_payload) => {
                        Reply::TopicsObserved(wire::TopicsObserved { topics: Vec::new() })
                    }
                    _ => Self::unimplemented_reply(),
                }
            }
            Observation::Records(query) => {
                let request = ObserveRecordsRequest {
                    optionTopic: None,
                    optionKind: None,
                };
                let action = match query.mode {
                    wire::ObservationMode::DescriptionOnly => {
                        ObserverAction::ObserveRecordsDescriptionOnly(request)
                    }
                    wire::ObservationMode::WithProvenance => {
                        ObserverAction::ObserveRecordsWithProvenance(request)
                    }
                };
                match self.observer.handle(action) {
                    ObserverResponse::RecordsObserved(_payload) => {
                        Reply::RecordsObserved(wire::RecordsObserved { records: Vec::new() })
                    }
                    ObserverResponse::RecordProvenancesObserved(_payload) => {
                        Reply::RecordProvenancesObserved(wire::RecordProvenancesObserved {
                            records: Vec::new(),
                        })
                    }
                    _ => Self::unimplemented_reply(),
                }
            }
            Observation::State => Self::unimplemented_reply(),
            Observation::Questions => Self::unimplemented_reply(),
        }
    }

    fn state_unimplemented(&self) -> Reply {
        Self::unimplemented_reply()
    }

    fn watch(&self, _subscription: Subscription) -> Reply {
        Self::unimplemented_reply()
    }

    fn unwatch(&self, _token: SubscriptionToken) -> Reply {
        Self::unimplemented_reply()
    }

    fn unimplemented_reply() -> Reply {
        Reply::RequestUnimplemented(wire::RequestUnimplemented {
            reason: wire::UnimplementedReason::NotBuiltYet,
        })
    }
}

impl From<&wire::Topics> for crate::spirit_recorder::Topics {
    fn from(topics: &wire::Topics) -> Self {
        crate::spirit_recorder::Topics(
            topics
                .as_slice()
                .iter()
                .map(|topic| crate::spirit_recorder::Topic(topic.as_str().to_string()))
                .collect(),
        )
    }
}

impl From<wire::Kind> for crate::spirit_recorder::Kind {
    fn from(kind: wire::Kind) -> Self {
        match kind {
            wire::Kind::Decision => Self::Decision,
            wire::Kind::Principle => Self::Principle,
            wire::Kind::Correction => Self::Correction,
            wire::Kind::Clarification => Self::Clarification,
            wire::Kind::Constraint => Self::Constraint,
        }
    }
}

impl From<Magnitude> for crate::spirit_recorder::Certainty {
    fn from(certainty: Magnitude) -> Self {
        match certainty {
            Magnitude::Minimum => Self::Minimum,
            Magnitude::VeryLow => Self::VeryLow,
            Magnitude::Low => Self::Low,
            Magnitude::Medium => Self::Medium,
            Magnitude::High => Self::High,
            Magnitude::VeryHigh => Self::VeryHigh,
            Magnitude::Maximum => Self::Maximum,
        }
    }
}

/// Extension trait — lets a Reply carry a "we also produced this"
/// trace without forking the daemon's reply type. Used only when an
/// unexpected actor response occurs (`Unknown(...)` for instance).
trait ReplyAlsoLog {
    fn also_log<T: std::fmt::Debug>(self, _other: T) -> Self;
}

impl ReplyAlsoLog for Reply {
    fn also_log<T: std::fmt::Debug>(self, _other: T) -> Self {
        // POC: no logging substrate; production daemon would push the
        // outlier into a structured log.
        self
    }
}
