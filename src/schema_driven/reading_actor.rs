//! Reading actor: the response dispatcher + logging tap from /346 §5.
//!
//! Per /346 §5 every actor's `handle(action) -> response` flows the
//! response back through the reading actor. The reading actor:
//!
//! - Forwards the response to its declared destination (wire reply,
//!   subscriber, tap-only log)
//! - Auto-taps every response into the logging facility (nothing is
//!   invisible)
//! - Owns its own ACTION + RESPONSE vocabulary via
//!   spirit-reading-actor.schema
//!
//! This is itself an actor --- not a hard-coded primitive. The
//! channel-contract pattern (one schema, one channel, one actor)
//! applies to the dispatch role.
//!
//! Types here mirror spirit-reading-actor.schema (cross-crate imports
//! prevent `emit_schema!` invocation today; see recorder.rs note).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::schema_driven::observer::ObserverResponse;
use crate::schema_driven::recorder::RecorderResponse;
use crate::schema_driven::supervisor::SupervisorResponse;

/// ACTION enum --- spirit-reading-actor.schema's `ReadingActorAction`.
#[derive(Debug, Clone)]
pub enum ReadingActorAction {
    DispatchRecorderResponse(DispatchEnvelope, RecorderResponse),
    DispatchObserverResponse(DispatchEnvelope, ObserverResponse),
    DispatchSupervisorResponse(DispatchEnvelope, SupervisorResponse),
    AttachLogSink(AttachLogSink),
    DetachLogSink(DetachLogSink),
}

/// RESPONSE enum --- spirit-reading-actor.schema's
/// `ReadingActorResponse` with the universal `Unknown` variant per
/// /346 §9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadingActorResponse {
    ResponseDispatched(DispatchOutcome),
    LogSinkAttached(u32),
    LogSinkDetached(u32),
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchEnvelope {
    pub correlation_token: u64,
    pub destination: DispatchDestination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchDestination {
    WireReply,
    Subscriber,
    Tap,
    LogOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    Delivered,
    LogOnly,
    DroppedAtFilter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachLogSink {
    pub identifier: u32,
    pub level: LogLevel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachLogSink {
    pub identifier: u32,
}

pub struct ReadingActor {
    log_sinks: Mutex<HashMap<u32, LogSinkRow>>,
    delivered: AtomicU64,
    tap_captures: Mutex<Vec<TapCapture>>,
}

#[derive(Debug, Clone)]
pub struct TapCapture {
    pub correlation_token: u64,
    pub destination: DispatchDestination,
    pub source: TapSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapSource {
    Recorder,
    Observer,
    Supervisor,
}

struct LogSinkRow {
    level: LogLevel,
}

impl ReadingActor {
    pub fn new() -> Self {
        Self {
            log_sinks: Mutex::new(HashMap::new()),
            delivered: AtomicU64::new(0),
            tap_captures: Mutex::new(Vec::new()),
        }
    }

    pub fn handle(&self, action: ReadingActorAction) -> ReadingActorResponse {
        match action {
            ReadingActorAction::DispatchRecorderResponse(envelope, _response) => {
                self.dispatch(envelope, TapSource::Recorder)
            }
            ReadingActorAction::DispatchObserverResponse(envelope, _response) => {
                self.dispatch(envelope, TapSource::Observer)
            }
            ReadingActorAction::DispatchSupervisorResponse(envelope, _response) => {
                self.dispatch(envelope, TapSource::Supervisor)
            }
            ReadingActorAction::AttachLogSink(attach) => self.handle_attach(attach),
            ReadingActorAction::DetachLogSink(detach) => self.handle_detach(detach),
        }
    }

    fn dispatch(&self, envelope: DispatchEnvelope, source: TapSource) -> ReadingActorResponse {
        // 1. forward to destination (no-op in stub: the actual sockets
        //    live in the daemon wire-up).
        let outcome = match envelope.destination {
            DispatchDestination::WireReply | DispatchDestination::Subscriber => {
                DispatchOutcome::Delivered
            }
            DispatchDestination::Tap | DispatchDestination::LogOnly => DispatchOutcome::LogOnly,
        };
        // 2. auto-tap: every response gets logged through the open
        //    sinks (the schema declares this as the universal
        //    `Tap LogSinkSet WriteEntry` fan-out per
        //    spirit-reading-actor.schema).
        self.tap_captures.lock().unwrap().push(TapCapture {
            correlation_token: envelope.correlation_token,
            destination: envelope.destination,
            source,
        });
        if matches!(outcome, DispatchOutcome::Delivered) {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        ReadingActorResponse::ResponseDispatched(outcome)
    }

    fn handle_attach(&self, attach: AttachLogSink) -> ReadingActorResponse {
        self.log_sinks.lock().unwrap().insert(
            attach.identifier,
            LogSinkRow {
                level: attach.level,
            },
        );
        ReadingActorResponse::LogSinkAttached(attach.identifier)
    }

    fn handle_detach(&self, detach: DetachLogSink) -> ReadingActorResponse {
        self.log_sinks.lock().unwrap().remove(&detach.identifier);
        ReadingActorResponse::LogSinkDetached(detach.identifier)
    }

    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(Ordering::SeqCst)
    }

    pub fn tap_captures(&self) -> Vec<TapCapture> {
        self.tap_captures.lock().unwrap().clone()
    }
}

impl Default for ReadingActor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_driven::recorder::RecorderResponse;

    #[test]
    fn log_sink_attach_and_detach() {
        let reading = ReadingActor::new();
        let attached = reading.handle(ReadingActorAction::AttachLogSink(AttachLogSink {
            identifier: 42,
            level: LogLevel::Info,
        }));
        assert!(matches!(
            attached,
            ReadingActorResponse::LogSinkAttached(42)
        ));
        let detached = reading.handle(ReadingActorAction::DetachLogSink(DetachLogSink {
            identifier: 42,
        }));
        assert!(matches!(
            detached,
            ReadingActorResponse::LogSinkDetached(42)
        ));
    }

    #[test]
    fn dispatch_records_tap_capture_and_increments_delivered() {
        let reading = ReadingActor::new();
        let response = reading.handle(ReadingActorAction::DispatchRecorderResponse(
            DispatchEnvelope {
                correlation_token: 7,
                destination: DispatchDestination::WireReply,
            },
            RecorderResponse::RecordAccepted(1),
        ));
        assert!(matches!(
            response,
            ReadingActorResponse::ResponseDispatched(DispatchOutcome::Delivered)
        ));
        assert_eq!(reading.delivered_count(), 1);
        let taps = reading.tap_captures();
        assert_eq!(taps.len(), 1);
        assert_eq!(taps[0].correlation_token, 7);
        assert!(matches!(taps[0].source, TapSource::Recorder));
    }

    #[test]
    fn log_only_destination_does_not_increment_delivered() {
        let reading = ReadingActor::new();
        let response = reading.handle(ReadingActorAction::DispatchObserverResponse(
            DispatchEnvelope {
                correlation_token: 8,
                destination: DispatchDestination::LogOnly,
            },
            ObserverResponse::NotificationDispatched(0),
        ));
        assert!(matches!(
            response,
            ReadingActorResponse::ResponseDispatched(DispatchOutcome::LogOnly)
        ));
        assert_eq!(reading.delivered_count(), 0);
        let taps = reading.tap_captures();
        assert_eq!(taps.len(), 1);
        assert!(matches!(taps[0].source, TapSource::Observer));
    }
}
