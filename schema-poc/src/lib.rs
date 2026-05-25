//! Schema-driven POC for persona-spirit (psyche 2026-05-26 + intent
//! records 709, 710 — the THREE-LANGUAGE structure).
//!
//! ## Three languages
//!
//! 1. **Wire signal language** — two emissions in two repos:
//!    - `signal-persona-spirit/spirit.schema` (public/ordinary socket)
//!    - `owner-signal-persona-spirit/owner-spirit.schema` (owner socket)
//!    Both projected through the schema-driven `emit_schema!()` macro
//!    in their respective signal crates, alongside the legacy
//!    `signal_channel!` emission.
//!
//! 2. **Storage sema language** — one schema in this crate:
//!    - `spirit-storage.schema` declares the redb table descriptors,
//!      the version marker, the StoredRecord shape, and the upgrade
//!      log entry shape.
//!
//! 3. **Internal channel languages** — one schema per major actor:
//!    - `spirit-recorder.schema`
//!    - `spirit-observer.schema`
//!    - `spirit-supervisor.schema`
//!    - `spirit-reading-actor.schema`
//!    - `spirit-upgrade-log.schema`
//!    Each declares ACTION + RESPONSE enums plus EffectTable +
//!    FanOutTargets. The composer injects `Unknown(String)` into each
//!    RESPONSE enum (the actor's safety floor).
//!
//! ## Actor engines
//!
//! Each actor's hand-written engine sits in its own module here. The
//! `handle(action) -> response` method is a closed Rust `match` on the
//! schema-emitted ACTION enum; Rust enforces exhaustiveness at compile
//! time. Errors return `<Actor>Response::Unknown(text)` — the safety
//! floor.
//!
//! ## Migration runner
//!
//! `SpiritStorageHandle::open` reads the on-disk VersionMarker and
//! runs the three-branch migration:
//!
//! - None        → fresh DB, write NEXT, log NoMigrationNeeded
//! - Some(NEXT)  → no-op, log NoMigrationNeeded
//! - Some(prev)  → run bridge → write NEXT → log MigratedSuccessfully

use signal_frame::emit_schema;

// Language 2 — STORAGE SEMA: redb tables + VersionMarker + StoredRecord.
emit_schema!("spirit-storage.schema");

// Language 3 — INTERNAL CHANNELS: one schema per actor.
emit_schema!("spirit-recorder.schema");
emit_schema!("spirit-observer.schema");
emit_schema!("spirit-supervisor.schema");
emit_schema!("spirit-reading-actor.schema");
emit_schema!("spirit-upgrade-log.schema");

// Modules with hand-written actor engines + the storage runner.
pub mod storage;
pub mod recorder;
pub mod observer;
pub mod supervisor;
pub mod reading_actor;
pub mod upgrade_log;
pub mod daemon;

pub use daemon::PocDaemon;
pub use storage::{
    MAIN_VERSION_MARKER, MigrationError, NEXT_VERSION_MARKER, SpiritStorageHandle,
    StorageLocation,
};
