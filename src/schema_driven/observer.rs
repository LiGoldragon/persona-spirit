//! SpiritObserver actor: schema-driven fan-out hub for live event
//! subscribers.
//!
//! Per /345 §3 the observer is an internal-channel actor: lives in
//! daemon process memory; ACTION + RESPONSE vocabulary is private to
//! the daemon; the wire-facing `Tap`/`Untap` operations are TRANSLATED
//! into observer actions by the dispatch layer.
//!
//! The actor holds the subscription table (subscriber -> filter) and
//! dispatches `PublishRecordCaptured` actions out to all matching
//! subscribers when records are captured upstream.
//!
//! Types here mirror spirit-observer.schema (cross-crate imports
//! prevent `emit_schema!` invocation today; see recorder.rs note).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// ACTION enum --- spirit-observer.schema's `ObserverAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverAction {
    OpenObserverSubscription(OpenObserverSubscription),
    CloseObserverSubscription(CloseObserverSubscription),
    PublishRecordCaptured(PublishRecordCaptured),
    QueryStatus,
}

/// RESPONSE enum --- spirit-observer.schema's `ObserverResponse` with
/// the universal `Unknown` variant injected per /346 §9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverResponse {
    ObserverSubscriptionOpened(u64),
    ObserverSubscriptionRetracted(u64),
    NotificationDispatched(u32),
    StatusReturned(ObserverStatus),
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenObserverSubscription {
    pub topic_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseObserverSubscription {
    pub token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecordCaptured {
    pub topic: String,
    pub identifier: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverStatus {
    pub subscribers_active: u32,
    pub notifications_dispatched: u64,
}

/// Hand-written engine state for the observer.
pub struct SpiritObserver {
    next_token: AtomicU64,
    subscriptions: Mutex<HashMap<u64, SubscriberRow>>,
    notifications_dispatched: AtomicU64,
}

/// Internal subscriber row. The filter shape is the wire's
/// `ObserverFilter` projected into the observer.
struct SubscriberRow {
    topic_filter: Option<String>,
}

impl SpiritObserver {
    pub fn new() -> Self {
        Self {
            next_token: AtomicU64::new(1),
            subscriptions: Mutex::new(HashMap::new()),
            notifications_dispatched: AtomicU64::new(0),
        }
    }

    pub fn handle(&self, action: ObserverAction) -> ObserverResponse {
        match action {
            ObserverAction::OpenObserverSubscription(open) => self.handle_open(open),
            ObserverAction::CloseObserverSubscription(close) => self.handle_close(close),
            ObserverAction::PublishRecordCaptured(publish) => self.handle_publish(publish),
            ObserverAction::QueryStatus => self.handle_status(),
        }
    }

    fn handle_open(&self, open: OpenObserverSubscription) -> ObserverResponse {
        let token = self.next_token.fetch_add(1, Ordering::SeqCst);
        self.subscriptions.lock().unwrap().insert(
            token,
            SubscriberRow {
                topic_filter: open.topic_filter,
            },
        );
        ObserverResponse::ObserverSubscriptionOpened(token)
    }

    fn handle_close(&self, close: CloseObserverSubscription) -> ObserverResponse {
        self.subscriptions.lock().unwrap().remove(&close.token);
        ObserverResponse::ObserverSubscriptionRetracted(close.token)
    }

    fn handle_publish(&self, publish: PublishRecordCaptured) -> ObserverResponse {
        let mut dispatched: u32 = 0;
        for row in self.subscriptions.lock().unwrap().values() {
            if row
                .topic_filter
                .as_ref()
                .is_none_or(|filter| filter == &publish.topic)
            {
                dispatched = dispatched.saturating_add(1);
            }
        }
        self.notifications_dispatched
            .fetch_add(u64::from(dispatched), Ordering::SeqCst);
        ObserverResponse::NotificationDispatched(dispatched)
    }

    fn handle_status(&self) -> ObserverResponse {
        let subscribers_active =
            u32::try_from(self.subscriptions.lock().unwrap().len()).unwrap_or(u32::MAX);
        ObserverResponse::StatusReturned(ObserverStatus {
            subscribers_active,
            notifications_dispatched: self.notifications_dispatched.load(Ordering::SeqCst),
        })
    }
}

impl Default for SpiritObserver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_lifecycle_round_trips_token() {
        let observer = SpiritObserver::new();
        let opened = observer.handle(ObserverAction::OpenObserverSubscription(
            OpenObserverSubscription { topic_filter: None },
        ));
        let token = match opened {
            ObserverResponse::ObserverSubscriptionOpened(token) => token,
            other => panic!("expected ObserverSubscriptionOpened, got {other:?}"),
        };
        let closed = observer.handle(ObserverAction::CloseObserverSubscription(
            CloseObserverSubscription { token },
        ));
        assert!(matches!(
            closed,
            ObserverResponse::ObserverSubscriptionRetracted(t) if t == token
        ));
    }

    #[test]
    fn publish_with_matching_filter_dispatches_to_all_subscribers() {
        let observer = SpiritObserver::new();
        observer.handle(ObserverAction::OpenObserverSubscription(
            OpenObserverSubscription {
                topic_filter: Some("intent".into()),
            },
        ));
        observer.handle(ObserverAction::OpenObserverSubscription(
            OpenObserverSubscription { topic_filter: None },
        ));
        let response = observer.handle(ObserverAction::PublishRecordCaptured(
            PublishRecordCaptured {
                topic: "intent".into(),
                identifier: 42,
            },
        ));
        // Both subscribers match (one filtered to intent, one
        // wildcard).
        assert!(matches!(
            response,
            ObserverResponse::NotificationDispatched(2)
        ));
    }

    #[test]
    fn publish_with_non_matching_filter_only_dispatches_to_wildcards() {
        let observer = SpiritObserver::new();
        observer.handle(ObserverAction::OpenObserverSubscription(
            OpenObserverSubscription {
                topic_filter: Some("statement".into()),
            },
        ));
        observer.handle(ObserverAction::OpenObserverSubscription(
            OpenObserverSubscription { topic_filter: None },
        ));
        let response = observer.handle(ObserverAction::PublishRecordCaptured(
            PublishRecordCaptured {
                topic: "intent".into(),
                identifier: 42,
            },
        ));
        // Only the wildcard subscriber matches.
        assert!(matches!(
            response,
            ObserverResponse::NotificationDispatched(1)
        ));
    }
}
