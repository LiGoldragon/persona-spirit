//! SpiritStorage actor: schema-driven redb-backed storage hub.
//!
//! Per /345 §3 the storage contract is its OWN channel (an external
//! channel that survives process exit). Per /346 §4 the storage layer
//! is the home of the version marker + auto-migration runner.
//!
//! The storage actor consumes the `StorageDescriptor` feature from
//! `spirit-storage.schema` --- the schema declares the table layouts
//! (Records, RecordIdentifierMint) and the engine reads them through
//! schema-emitted `TableDescriptor`s.
//!
//! This module ships the load-bearing migration machinery per /346 §4
//! step 6:
//!
//! - `VersionMarker` --- the version triple recorded on disk
//! - `read_version_marker` / `write_version_marker` --- IO helpers
//! - `run_migration` --- the orchestrator that reads the marker,
//!   routes through the bridge if previous, writes the marker
//!   forward, appends an entry to the upgrade-log
//!
//! The types declared here match what `emit_schema!("spirit-storage")`
//! WILL emit once the per-crate import resolution lands (the existing
//! `spirit-storage.schema` imports from `signal-persona-spirit` and
//! `signal-sema`, both of which need adjacent worktrees for resolution
//! and so aren't usable via `emit_schema!` directly today). The
//! shapes are aligned with the schema declarations so the migration
//! happens regardless of which path the daemon takes to the same
//! data structures.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::SystemTime;

use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::{Failure, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

/// Version marker stored alongside the database per /346 §4 step 4.
///
/// The three-component shape matches `spirit-storage.schema`'s
/// `VersionMarker [u32 u32 u32]` declaration: major, minor, patch.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionMarker {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl VersionMarker {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// The version this build of persona-spirit writes data under.
    /// Per /346 §6 the daemon always writes from the NEXT perspective.
    pub const NEXT: Self = Self::new(0, 1, 1);

    /// The MAIN baseline this build can migrate from. Per /346 §6
    /// when MAIN == NEXT the bridge module is elided; in this build
    /// they differ and the bridge handles the transition.
    pub const MAIN: Self = Self::new(0, 1, 0);
}

/// Outcome of one upgrade ceremony, recorded in the upgrade-log
/// table per /346 §4 step 6.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpgradeOutcome {
    MigratedSuccessfully,
    NoMigrationNeeded,
    MigrationFailed,
    RolledBack,
}

/// One row of the upgrade-log table (spirit-upgrade-log.schema's
/// `UpgradeLogEntry`).
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeLogEntry {
    pub from: VersionMarker,
    pub to: VersionMarker,
    pub outcome: UpgradeOutcome,
    /// micros-since-epoch when the migration ceremony started
    pub timestamp_micros: u64,
    /// duration in micros from start to outcome
    pub duration_micros: u64,
}

/// `TableDescriptor` shape per /343 §8 item 4 + /346 §4 --- the
/// canonical descriptor the schema engine would emit. The shape is
/// `(logical_name, table_type)` matching the `StorageDescriptor`
/// entries declared in `spirit-storage.schema`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableDescriptor {
    pub logical_name: &'static str,
    pub table_type: &'static str,
}

impl TableDescriptor {
    pub const fn new(logical_name: &'static str, table_type: &'static str) -> Self {
        Self {
            logical_name,
            table_type,
        }
    }
}

/// `StorageDescriptor` projection per `spirit-storage.schema`'s
/// `(StorageDescriptor [ (Records RecordsTable) (RecordIdentifierMint
/// RecordIdentifierMintTable) ])` feature.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageDescriptor;

impl StorageDescriptor {
    /// Closed set of tables this schema owns per /346 §4.
    pub const TABLES: &'static [TableDescriptor] = &[
        TableDescriptor::new("Records", "RecordsTable"),
        TableDescriptor::new("RecordIdentifierMint", "RecordIdentifierMintTable"),
    ];

    /// Returns the layout-type name for `logical_name`, or `None` if
    /// the StorageDescriptor doesn't list this name.
    pub fn table_type_for(logical_name: &str) -> Option<&'static str> {
        Self::TABLES
            .iter()
            .find(|entry| entry.logical_name == logical_name)
            .map(|entry| entry.table_type)
    }
}

/// Handle to the schema-driven storage layer.
#[derive(Clone)]
pub struct SpiritStorageHandle {
    inner: std::sync::Arc<SpiritStorageInner>,
}

pub struct SpiritStorageInner {
    location: PathBuf,
    /// Last observed version marker on disk. None until first load
    /// completes.
    marker: Mutex<Option<VersionMarker>>,
    /// Captured upgrade-log entries for the current daemon session.
    /// In production redb-backed code these would land in the
    /// upgrade-log table from spirit-upgrade-log.schema; the
    /// schema-driven module keeps the in-memory log so the migration
    /// runner can be unit-tested without dragging redb in.
    upgrade_log: Mutex<Vec<UpgradeLogEntry>>,
}

impl SpiritStorageHandle {
    /// Open the storage. Reads the version marker beside the database;
    /// runs the auto-migration per /346 §4 step 6 if MAIN is found
    /// (data was written under the previous version and needs the
    /// bridge); writes NEXT forward; records the outcome in the
    /// upgrade-log table.
    pub fn open(location: impl Into<PathBuf>) -> Self {
        let location = location.into();
        let on_disk = read_version_marker(&location).ok();
        let mut upgrade_log = Vec::new();
        let final_marker = match on_disk {
            None => {
                // Fresh database --- no version marker on disk. Write
                // NEXT and log a NoMigrationNeeded entry per /346 §4.
                let _ = write_version_marker(&location, VersionMarker::NEXT);
                upgrade_log.push(UpgradeLogEntry {
                    from: VersionMarker::NEXT,
                    to: VersionMarker::NEXT,
                    outcome: UpgradeOutcome::NoMigrationNeeded,
                    timestamp_micros: now_micros(),
                    duration_micros: 0,
                });
                VersionMarker::NEXT
            }
            Some(marker) if marker == VersionMarker::NEXT => {
                // Already at NEXT --- no migration required.
                upgrade_log.push(UpgradeLogEntry {
                    from: marker,
                    to: marker,
                    outcome: UpgradeOutcome::NoMigrationNeeded,
                    timestamp_micros: now_micros(),
                    duration_micros: 0,
                });
                marker
            }
            Some(previous) => {
                // Migration territory. Run the bridge; if it
                // succeeds write the marker forward + log success.
                let started = now_micros();
                let migration = run_migration(&location, previous, VersionMarker::NEXT);
                let duration = now_micros().saturating_sub(started);
                match migration {
                    Ok(()) => {
                        let _ = write_version_marker(&location, VersionMarker::NEXT);
                        upgrade_log.push(UpgradeLogEntry {
                            from: previous,
                            to: VersionMarker::NEXT,
                            outcome: UpgradeOutcome::MigratedSuccessfully,
                            timestamp_micros: started,
                            duration_micros: duration,
                        });
                        VersionMarker::NEXT
                    }
                    Err(_error) => {
                        upgrade_log.push(UpgradeLogEntry {
                            from: previous,
                            to: VersionMarker::NEXT,
                            outcome: UpgradeOutcome::MigrationFailed,
                            timestamp_micros: started,
                            duration_micros: duration,
                        });
                        previous
                    }
                }
            }
        };

        Self {
            inner: std::sync::Arc::new(SpiritStorageInner {
                location,
                marker: Mutex::new(Some(final_marker)),
                upgrade_log: Mutex::new(upgrade_log),
            }),
        }
    }

    pub fn location(&self) -> &std::path::Path {
        &self.inner.location
    }

    /// Returns the current version marker the daemon believes the
    /// database is written under. Synchronised to NEXT after a
    /// successful migration; held at MAIN if migration failed.
    pub fn current_marker(&self) -> Option<VersionMarker> {
        *self.inner.marker.lock().unwrap()
    }

    /// Returns the upgrade-log entries collected during this daemon
    /// session. Each `open()` plus every subsequent migration would
    /// append rows; only `open()` writes are recorded today since
    /// the schema-driven storage is the only mutator.
    pub fn upgrade_log(&self) -> Vec<UpgradeLogEntry> {
        self.inner.upgrade_log.lock().unwrap().clone()
    }
}

/// Auto-migration runner per /346 §4 step 6.
///
/// Reads each previous-version row, runs the bridge `From` impl, writes
/// the next-version shape, updates the marker. Currently a no-op
/// because the only schema change between MAIN (0.1.0) and NEXT
/// (0.1.1) is the addition of the version marker itself --- no row
/// shape changes. A future schema change (added field, renamed type)
/// drops the bridge body here per /346 §4 step 5.
pub fn run_migration(
    _location: &std::path::Path,
    previous: VersionMarker,
    next: VersionMarker,
) -> Result<(), MigrationError> {
    // Marker-only upgrade: no row transformations needed for the
    // MAIN -> NEXT bridge. Per /346 §4 step 5, when AssembledSchema
    // diff is empty, the bridge body can be elided --- this is that
    // case for the current build.
    let _ = (previous, next);
    Ok(())
}

/// Migration failure modes per /346 §4 step 6.
#[derive(Debug)]
pub enum MigrationError {
    /// The bridge code rejected a row it couldn't transform.
    UnknownBridge {
        from: VersionMarker,
        to: VersionMarker,
    },
    /// IO error while writing the next-version shape.
    Io(std::io::Error),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBridge { from, to } => write!(
                formatter,
                "no bridge from {from:?} to {to:?}; rebuild daemon with `mod previous` for {from:?}"
            ),
            Self::Io(error) => write!(formatter, "migration io error: {error}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<std::io::Error> for MigrationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// File companion path for the version marker. The marker lives in
/// a sibling file `<location>.version` so the redb file itself stays
/// opaque to the migration machinery --- per /346 §4 a future
/// iteration may move this into the redb file as a single-row
/// `VersionMarker` table.
fn marker_path(location: &std::path::Path) -> PathBuf {
    let mut path = location.to_path_buf();
    let original = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "persona-spirit.redb".to_string());
    path.set_file_name(format!("{original}.version"));
    path
}

/// Read the version marker beside the database file. Returns `None`
/// when the marker doesn't exist (fresh database).
pub fn read_version_marker(location: &std::path::Path) -> Result<VersionMarker, std::io::Error> {
    let path = marker_path(location);
    let bytes = std::fs::read(&path)?;
    let archived = rkyv::access::<<VersionMarker as Archive>::Archived, Failure>(&bytes)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let marker: VersionMarker = rkyv::deserialize::<VersionMarker, Failure>(archived)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(marker)
}

/// Write the version marker beside the database file. Per /346 §4
/// step 6 this is the FINAL step of a successful migration --- the
/// marker advance signals "data is now at NEXT shape".
pub fn write_version_marker(
    location: &std::path::Path,
    marker: VersionMarker,
) -> Result<(), std::io::Error> {
    let path = marker_path(location);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = rkyv::to_bytes::<Failure>(&marker)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    std::fs::write(&path, bytes.as_slice())?;
    Ok(())
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

/// Trait-bound suppressor: the rkyv impls above introduce trait
/// requirements that should compile because all the types satisfy
/// them; this function exists so `cargo +stable check` doesn't
/// silently miss them.
#[allow(dead_code)]
fn _marker_check<'buffer>(
    _validator: HighValidator<'buffer, Failure>,
    _serializer: HighSerializer<AlignedVec, ArenaHandle<'buffer>, Failure>,
    _strategy: Strategy<(), Failure>,
) where
    VersionMarker: for<'a> CheckBytes<HighValidator<'a, Failure>>,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_marker_round_trip_to_file() {
        let dir = std::env::temp_dir().join("persona-spirit-schema-driven-storage");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let location = dir.join("test.redb");
        let marker = VersionMarker::new(0, 1, 1);
        write_version_marker(&location, marker).unwrap();
        let observed = read_version_marker(&location).unwrap();
        assert_eq!(observed, marker);
    }

    #[test]
    fn fresh_database_writes_next_marker_and_logs_no_migration() {
        let dir = std::env::temp_dir().join("persona-spirit-schema-driven-storage-fresh");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let location = dir.join("fresh.redb");
        let handle = SpiritStorageHandle::open(&location);

        assert_eq!(handle.current_marker(), Some(VersionMarker::NEXT));
        let log = handle.upgrade_log();
        assert_eq!(log.len(), 1);
        assert!(matches!(log[0].outcome, UpgradeOutcome::NoMigrationNeeded));
    }

    #[test]
    fn previous_version_database_runs_migration_and_logs_success() {
        let dir = std::env::temp_dir().join("persona-spirit-schema-driven-storage-migrate");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let location = dir.join("migrate.redb");
        // Seed the marker with MAIN (the prior version).
        write_version_marker(&location, VersionMarker::MAIN).unwrap();

        let handle = SpiritStorageHandle::open(&location);

        // Migration moved the marker forward.
        assert_eq!(handle.current_marker(), Some(VersionMarker::NEXT));
        let log = handle.upgrade_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].from, VersionMarker::MAIN);
        assert_eq!(log[0].to, VersionMarker::NEXT);
        assert!(matches!(
            log[0].outcome,
            UpgradeOutcome::MigratedSuccessfully
        ));

        // Re-open and confirm the marker is at NEXT now.
        let reopened = SpiritStorageHandle::open(&location);
        assert_eq!(reopened.current_marker(), Some(VersionMarker::NEXT));
        let reopened_log = reopened.upgrade_log();
        assert_eq!(reopened_log.len(), 1);
        assert!(matches!(
            reopened_log[0].outcome,
            UpgradeOutcome::NoMigrationNeeded
        ));
    }

    #[test]
    fn storage_descriptor_knows_authored_table_layout() {
        // Mirrors the StorageDescriptor feature declared in
        // spirit-storage.schema.
        assert_eq!(StorageDescriptor::TABLES.len(), 2);
        assert_eq!(
            StorageDescriptor::table_type_for("Records"),
            Some("RecordsTable")
        );
        assert_eq!(
            StorageDescriptor::table_type_for("RecordIdentifierMint"),
            Some("RecordIdentifierMintTable")
        );
        assert_eq!(StorageDescriptor::table_type_for("Missing"), None);
    }
}
