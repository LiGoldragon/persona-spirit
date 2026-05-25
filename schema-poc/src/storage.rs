//! Storage actor + auto-migration runner (psyche 2026-05-26 + intent
//! records 709, 710 — STORAGE SEMA LANGUAGE).
//!
//! `SpiritStorageHandle::open` is the **three-branch migration
//! runner** per /346 §4:
//!
//! - **None**       → fresh DB, write NEXT, log NoMigrationNeeded.
//! - **Some(NEXT)** → no-op, log NoMigrationNeeded.
//! - **Some(prev)** → run bridge → write NEXT → log
//!                    MigratedSuccessfully.
//!
//! The schema-emitted `VersionMarker` is the boundary token.
//!
//! For the POC the storage is **in-memory** — a `Mutex<...>` wrapping
//! the records vec. Mirroring the schema's `StorageDescriptor`, four
//! logical tables exist: `Records`, `IdentifierMint`, `VersionMarker`,
//! `UpgradeLog`. The redb wiring is a future operator slice
//! (`/346 §4 step 6`); the migration runner's three-branch shape and
//! the universal-Unknown floor on every actor's RESPONSE are the
//! load-bearing POC claims.

use std::sync::Mutex;

use crate::spirit_storage::{StorageDescriptor, UpgradeOutcome, VersionMarker};

/// MAIN version marker — the published baseline. The POC's NEXT is one
/// patch ahead so the migration runner exercises a real previous→next
/// transition (instead of the no-op fresh-DB branch only).
pub const MAIN_VERSION_MARKER: VersionMarker = VersionMarker {
    u32: 0,
    u32_2: 3,
    u32_3: 0,
};

/// NEXT version marker — what the POC daemon writes on fresh opens
/// and what the migration runner targets.
pub const NEXT_VERSION_MARKER: VersionMarker = VersionMarker {
    u32: 0,
    u32_2: 3,
    u32_3: 1,
};

/// Where the storage lives on disk. The POC uses an in-memory marker
/// instead of an actual redb path; future operator slice wires the
/// path through to a real redb file.
#[derive(Clone, Debug)]
pub enum StorageLocation {
    InMemory,
}

/// The storage handle the daemon holds. Carries the on-open
/// VersionMarker plus an in-memory `UpgradeLog` of every open's
/// outcome.
#[derive(Debug)]
pub struct SpiritStorageHandle {
    location: StorageLocation,
    current_marker: Mutex<VersionMarker>,
    upgrade_log: Mutex<Vec<UpgradeLogRecord>>,
    next_identifier: Mutex<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpgradeLogRecord {
    pub from: VersionMarker,
    pub to: VersionMarker,
    pub outcome: UpgradeOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationError {
    BridgeFailed,
}

impl SpiritStorageHandle {
    /// Fresh-DB open: writes NEXT, logs `NoMigrationNeeded`.
    /// Used as the POC's most common open path; mirrors v0.3's daemon
    /// startup-on-empty-redb branch.
    pub fn open(location: StorageLocation) -> Result<Self, MigrationError> {
        Self::open_with_on_disk_marker(location, None)
    }

    /// Open with explicit on-disk marker — used by the migration
    /// constraint tests. Exposes the three-branch migration logic
    /// directly without needing a real redb fixture.
    pub fn open_with_on_disk_marker(
        location: StorageLocation,
        on_disk: Option<VersionMarker>,
    ) -> Result<Self, MigrationError> {
        let mut log = Vec::new();
        let final_marker = match on_disk {
            None => {
                // Fresh DB: write NEXT, log NoMigrationNeeded.
                log.push(UpgradeLogRecord {
                    from: NEXT_VERSION_MARKER,
                    to: NEXT_VERSION_MARKER,
                    outcome: UpgradeOutcome::NoMigrationNeeded,
                });
                NEXT_VERSION_MARKER
            }
            Some(marker) if marker == NEXT_VERSION_MARKER => {
                // Already-NEXT: no-op.
                log.push(UpgradeLogRecord {
                    from: marker.clone(),
                    to: marker.clone(),
                    outcome: UpgradeOutcome::NoMigrationNeeded,
                });
                marker
            }
            Some(previous) => {
                // Migration: run bridge → write NEXT → log
                // MigratedSuccessfully.
                Self::run_migration(previous.clone(), NEXT_VERSION_MARKER)?;
                log.push(UpgradeLogRecord {
                    from: previous,
                    to: NEXT_VERSION_MARKER,
                    outcome: UpgradeOutcome::MigratedSuccessfully,
                });
                NEXT_VERSION_MARKER
            }
        };
        Ok(Self {
            location,
            current_marker: Mutex::new(final_marker),
            upgrade_log: Mutex::new(log),
            next_identifier: Mutex::new(1),
        })
    }

    pub fn location(&self) -> &StorageLocation {
        &self.location
    }

    pub fn current_marker(&self) -> VersionMarker {
        self
            .current_marker
            .lock()
            .expect("storage marker mutex poisoned")
            .clone()
    }

    pub fn upgrade_log(&self) -> Vec<UpgradeLogRecord> {
        self.upgrade_log
            .lock()
            .expect("upgrade log mutex poisoned")
            .clone()
    }

    /// Mint a fresh monotonic identifier. Atomic across the actors
    /// that share the handle; mirrors the v0.3 IdentifierMintTable
    /// behaviour in-memory.
    pub fn mint_identifier(&self) -> u64 {
        let mut guard = self
            .next_identifier
            .lock()
            .expect("identifier mint mutex poisoned");
        let value = *guard;
        *guard += 1;
        value
    }

    /// Storage descriptor names this handle owns — proves the schema-
    /// emitted `StorageDescriptor` is reachable.
    pub fn descriptor_table_names() -> Vec<&'static str> {
        StorageDescriptor::TABLES
            .iter()
            .map(|table| table.logical_name)
            .collect()
    }

    /// The minimal migration bridge: MAIN → NEXT differs only in patch
    /// version, no row transformations needed. Real bridges between
    /// version boundaries with row reshapes live as `mod previous` →
    /// `mod next` modules per /346 §4 step 5.
    fn run_migration(
        previous: VersionMarker,
        next: VersionMarker,
    ) -> Result<(), MigrationError> {
        // Marker-only upgrade: no row transformations needed for the
        // MAIN → NEXT bridge. Per /346 §6 the bridge body is elided
        // when the AssembledSchema diff is empty.
        let _ = (previous, next);
        Ok(())
    }
}
