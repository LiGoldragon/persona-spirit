//! Schema-driven engine substrate for the spirit daemon.
//!
//! This module demonstrates the actor pattern from /346 + /345 §7 on
//! top of the multi-schema-per-crate layout (spirit-storage,
//! spirit-recorder, spirit-observer, spirit-supervisor,
//! spirit-reading-actor) authored alongside this crate.
//!
//! Each actor consumes the ACTION + RESPONSE enums emitted from its
//! own .schema file. The match block IS the contact point (per
//! `skills/enum-contact-points.md`) between the inbound channel
//! (ACTION) and the outbound channel (RESPONSE).
//!
//! ### Hand-written vs schema-emitted types
//!
//! The schemas in this crate import from sibling crates
//! (`signal-persona-spirit`, `signal-sema`, `spirit-storage`,
//! `spirit-recorder`, ...) which the in-tree schema reader cannot
//! follow without an adjacent worktree layout that mirrors the
//! deployed repo tree. Cross-crate schema-resolution is a deferred
//! piece of infrastructure (operator slice).
//!
//! Until that lands the ACTION + RESPONSE types here are HAND-WRITTEN
//! to MATCH what `emit_schema!` will produce once the resolution
//! infrastructure is in place. The shapes deliberately mirror the
//! schemas exactly:
//!
//! - Each actor's ACTION enum carries the closed set of variants
//!   declared in the corresponding .schema's namespace.
//! - Each actor's RESPONSE enum carries the same plus the universal
//!   `Unknown(String)` variant injected by `UniversalUnknownMacro` per
//!   /346 §9.
//! - Payload structs mirror the schema field lists.
//!
//! When `emit_schema!` lights up, the hand-written types here drop
//! and the macro invocations below take their place. The actor
//! engines (handle methods, internal state) stay; only the type
//! definitions migrate.
//!
//! ### What this module DOES land today
//!
//! - Working actor engines (recorder / observer / supervisor /
//!   reading_actor) with handle methods, internal state, and
//!   universal-Unknown safety floor on every RESPONSE per /346 §1+§9.
//! - Working `SpiritStorageHandle` with auto-migration runner per
//!   /346 §4 step 6 (reads version marker, runs bridge, writes
//!   marker forward, logs to upgrade-log).
//! - Unit tests proving the migration runner advances the marker
//!   and records the outcome (see `storage::tests`).

pub mod observer;
pub mod reading_actor;
pub mod recorder;
pub mod storage;
pub mod supervisor;

// Once cross-crate schema-resolution lands, these will replace the
// hand-written types in the modules above:
//
//   schema_rust::emit_schema!("spirit-storage.schema");
//   schema_rust::emit_schema!("spirit-recorder.schema");
//   schema_rust::emit_schema!("spirit-observer.schema");
//   schema_rust::emit_schema!("spirit-supervisor.schema");
//   schema_rust::emit_schema!("spirit-reading-actor.schema");
//   schema_rust::emit_schema!("spirit-upgrade-log.schema");
//
// Tracking item: schema cross-crate resolution + the
// `signal_rust::emit_schema!` proc-macro's import-path resolution.
