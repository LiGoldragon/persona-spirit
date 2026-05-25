//! Supervisor actor (psyche 2026-05-26 — INTERNAL CHANNEL LANGUAGE).
//! Owns lifecycle: bring up actors, drain them, manage the
//! owner-channel-derived StartCommand / DrainCommand / Reload.

use crate::spirit_supervisor::{
    DrainEngineRequest, EngineDrained, EngineStarted, EngineStatus, EngineStatusReturned,
    Generation, QueryEngineStatusRequest, StartEngineRequest, SupervisorAction,
    SupervisorResponse,
};

#[derive(Clone, Default)]
pub struct SpiritSupervisor;

impl SpiritSupervisor {
    pub fn new() -> Self {
        Self
    }

    pub fn handle(&self, action: SupervisorAction) -> SupervisorResponse {
        match action {
            SupervisorAction::StartEngine(request) => self.start_engine(request),
            SupervisorAction::DrainEngine(request) => self.drain_engine(request),
            SupervisorAction::QueryEngineStatus(request) => self.query_status(request),
        }
    }

    fn start_engine(&self, request: StartEngineRequest) -> SupervisorResponse {
        let StartEngineRequest(generation) = request;
        SupervisorResponse::EngineStarted(EngineStarted(generation))
    }

    fn drain_engine(&self, request: DrainEngineRequest) -> SupervisorResponse {
        let DrainEngineRequest(generation) = request;
        SupervisorResponse::EngineDrained(EngineDrained(generation))
    }

    fn query_status(&self, request: QueryEngineStatusRequest) -> SupervisorResponse {
        let QueryEngineStatusRequest(Generation(generation_value)) = request;
        SupervisorResponse::EngineStatusReturned(EngineStatusReturned(EngineStatus {
            u8: 1,
            u64: generation_value,
        }))
    }
}
