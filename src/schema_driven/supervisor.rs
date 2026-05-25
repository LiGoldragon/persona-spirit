//! SpiritSupervisor actor: top-of-process lifecycle authority.
//!
//! Per `skills/enum-contact-points.md` the supervisor is the mediator
//! actor: owns engine startup, drain, and the bootstrap-policy reload.
//! The supervisor coordinates the cross-actor drain --- recorder,
//! observer, storage all close cleanly through the supervisor's
//! `DrainEngine` action.
//!
//! The supervisor IS the actor downstream of the owner-channel; the
//! owner-channel's `Start`/`Drain`/`Reload`/`Register`/`Retire`
//! operations TRANSLATE into supervisor actions.
//!
//! Types here mirror spirit-supervisor.schema (cross-crate imports
//! prevent `emit_schema!` invocation today; see recorder.rs note).

#![allow(dead_code)]

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// ACTION enum --- spirit-supervisor.schema's `SupervisorAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorAction {
    StartEngine(StartEngine),
    DrainEngine(DrainEngine),
    ReloadBootstrapPolicy(ReloadBootstrapPolicy),
    RegisterOwner(RegisterOwner),
    RetireOwner(RetireOwner),
    QueryEngineStatus,
}

/// RESPONSE enum --- spirit-supervisor.schema's `SupervisorResponse`
/// with the universal `Unknown` variant per /346 §9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorResponse {
    EngineStarted(EngineState),
    EngineDrained(EngineState),
    BootstrapPolicyReloaded(PolicySnapshot),
    OwnerRegistered(u64),
    OwnerRetired(u64),
    EngineStatusReturned(EngineState),
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartEngine {
    pub reason: StartReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainEngine {
    pub reason: DrainReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadBootstrapPolicy {
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterOwner {
    pub identifier: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireOwner {
    pub identifier: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartReason {
    FreshStart,
    Resume,
    HandoverFromPrevious,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainReason {
    ShuttingDown,
    HandoverToNext,
    PolicyChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineLifecycleState {
    Starting,
    Running,
    Draining,
    Drained,
    Halted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineState {
    pub lifecycle: EngineLifecycleState,
    pub uptime_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub source: String,
}

pub struct SpiritSupervisor {
    lifecycle_state: Mutex<EngineLifecycleState>,
    started_at: AtomicU64,
    policy_source: Mutex<Option<String>>,
}

impl SpiritSupervisor {
    pub fn new() -> Self {
        Self {
            lifecycle_state: Mutex::new(EngineLifecycleState::Starting),
            started_at: AtomicU64::new(0),
            policy_source: Mutex::new(None),
        }
    }

    pub fn handle(&self, action: SupervisorAction) -> SupervisorResponse {
        match action {
            SupervisorAction::StartEngine(start) => self.handle_start(start),
            SupervisorAction::DrainEngine(drain) => self.handle_drain(drain),
            SupervisorAction::ReloadBootstrapPolicy(reload) => self.handle_reload(reload),
            SupervisorAction::RegisterOwner(register) => self.handle_register(register),
            SupervisorAction::RetireOwner(retire) => self.handle_retire(retire),
            SupervisorAction::QueryEngineStatus => self.handle_query_status(),
        }
    }

    fn handle_start(&self, _start: StartEngine) -> SupervisorResponse {
        let now = now_micros();
        self.started_at.store(now, Ordering::SeqCst);
        let mut state = self.lifecycle_state.lock().unwrap();
        *state = EngineLifecycleState::Running;
        SupervisorResponse::EngineStarted(EngineState {
            lifecycle: *state,
            uptime_micros: 0,
        })
    }

    fn handle_drain(&self, _drain: DrainEngine) -> SupervisorResponse {
        // Cross-actor fan-out per spirit-supervisor.schema's
        // EngineDrainEffect: recorder.drain, observer.drain,
        // storage.drain. The actual fan-out happens in the daemon
        // wire-up; here we transition the supervisor's own state.
        let uptime = now_micros().saturating_sub(self.started_at.load(Ordering::SeqCst));
        let mut state = self.lifecycle_state.lock().unwrap();
        *state = EngineLifecycleState::Drained;
        SupervisorResponse::EngineDrained(EngineState {
            lifecycle: *state,
            uptime_micros: uptime,
        })
    }

    fn handle_reload(&self, reload: ReloadBootstrapPolicy) -> SupervisorResponse {
        *self.policy_source.lock().unwrap() = Some(reload.source.clone());
        SupervisorResponse::BootstrapPolicyReloaded(PolicySnapshot {
            source: reload.source,
        })
    }

    fn handle_register(&self, register: RegisterOwner) -> SupervisorResponse {
        SupervisorResponse::OwnerRegistered(register.identifier)
    }

    fn handle_retire(&self, retire: RetireOwner) -> SupervisorResponse {
        SupervisorResponse::OwnerRetired(retire.identifier)
    }

    fn handle_query_status(&self) -> SupervisorResponse {
        let uptime = now_micros().saturating_sub(self.started_at.load(Ordering::SeqCst));
        let lifecycle = *self.lifecycle_state.lock().unwrap();
        SupervisorResponse::EngineStatusReturned(EngineState {
            lifecycle,
            uptime_micros: uptime,
        })
    }
}

impl Default for SpiritSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_transitions_to_running() {
        let supervisor = SpiritSupervisor::new();
        let response = supervisor.handle(SupervisorAction::StartEngine(StartEngine {
            reason: StartReason::FreshStart,
        }));
        match response {
            SupervisorResponse::EngineStarted(state) => {
                assert!(matches!(state.lifecycle, EngineLifecycleState::Running))
            }
            other => panic!("expected EngineStarted, got {other:?}"),
        }
    }

    #[test]
    fn drain_transitions_to_drained() {
        let supervisor = SpiritSupervisor::new();
        supervisor.handle(SupervisorAction::StartEngine(StartEngine {
            reason: StartReason::FreshStart,
        }));
        let response = supervisor.handle(SupervisorAction::DrainEngine(DrainEngine {
            reason: DrainReason::ShuttingDown,
        }));
        match response {
            SupervisorResponse::EngineDrained(state) => {
                assert!(matches!(state.lifecycle, EngineLifecycleState::Drained))
            }
            other => panic!("expected EngineDrained, got {other:?}"),
        }
    }

    #[test]
    fn reload_remembers_policy_source() {
        let supervisor = SpiritSupervisor::new();
        let response = supervisor.handle(SupervisorAction::ReloadBootstrapPolicy(
            ReloadBootstrapPolicy {
                source: "/etc/persona/bootstrap.nota".into(),
            },
        ));
        match response {
            SupervisorResponse::BootstrapPolicyReloaded(snapshot) => {
                assert_eq!(snapshot.source, "/etc/persona/bootstrap.nota");
            }
            other => panic!("expected BootstrapPolicyReloaded, got {other:?}"),
        }
    }
}
