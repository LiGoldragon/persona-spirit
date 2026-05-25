use std::fs;

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use sema::SchemaVersion;
use sema_engine::{
    Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
};
use signal_persona_spirit::{
    Date, Entry, RecordIdentifier, Time,
    migration::{V010ToV011, v010},
};
use version_projection::VersionProjection;

use crate::{
    Error, Result, StoreLocation, StorePath,
    store::{SpiritStore, StampedEntry},
};

const V010_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const RECORDS: TableName = TableName::new("records");

#[derive(Debug, Clone, PartialEq, Eq, NotaRecord)]
pub struct MigrationConfiguration {
    pub source: StorePath,
    pub target: StorePath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationCompleted {
    pub records: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationOutcome {
    records: u64,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V010StoredRecord {
    identifier: RecordIdentifier,
    entry: V010StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V010StampedEntry {
    entry: v010::Entry,
    date: Date,
    time: Time,
}

impl MigrationConfiguration {
    pub fn new(source: StorePath, target: StorePath) -> Self {
        Self { source, target }
    }

    pub fn from_argument(argument: signal_frame::SingleArgument) -> Result<Self> {
        Self::from_text(&migration_configuration_argument_text(argument)?)
    }

    pub fn from_text(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let configuration = Self::decode(&mut decoder).map_err(Error::invalid_spirit_request)?;
        if let Some(token) = decoder
            .peek_token()
            .map_err(Error::invalid_spirit_request)?
        {
            return Err(Error::InvalidSpiritRequest {
                reason: format!("expected end of input, got {token:?}"),
            });
        }
        Ok(configuration)
    }

    pub fn migrate(self) -> Result<MigrationOutcome> {
        migrate_v010_to_v020(&self.source, &self.target)
    }
}

impl MigrationCompleted {
    pub const fn new(records: u64) -> Self {
        Self { records }
    }
}

impl MigrationOutcome {
    pub const fn new(records: u64) -> Self {
        Self { records }
    }

    pub const fn records(self) -> u64 {
        self.records
    }

    pub const fn completed(self) -> MigrationCompleted {
        MigrationCompleted::new(self.records)
    }
}

pub fn migrate_v010_to_v020(source: &StorePath, target: &StorePath) -> Result<MigrationOutcome> {
    let source_records = V010Store::open(source)?.all_records()?;
    let target_store = SpiritStore::open(&StoreLocation::new(target.as_path()))?;
    if !target_store.is_empty()? {
        return Err(Error::migration(
            "target v0.2 database must be empty before timestamp-preserving migration",
        ));
    }

    let mut migrated = 0;
    for record in source_records {
        target_store.import_migrated_record(record.identifier, record.project()?)?;
        migrated += 1;
    }
    Ok(MigrationOutcome::new(migrated))
}

impl NotaEncode for MigrationCompleted {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        encoder.start_record("MigrationCompleted")?;
        self.records.encode(encoder)?;
        encoder.end_record()
    }
}

struct V010Store {
    engine: Engine,
    records: sema_engine::TableReference<V010StoredRecord>,
}

impl V010Store {
    fn open(path: &StorePath) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(path.as_path(), V010_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn all_records(&self) -> Result<Vec<V010StoredRecord>> {
        let mut records = self
            .engine
            .match_records(QueryPlan::all(self.records))
            .map_err(Error::spirit_store)?
            .records()
            .to_vec();
        records.sort_by_key(|record| record.identifier.value());
        Ok(records)
    }
}

impl V010StoredRecord {
    fn project(self) -> Result<StampedEntry> {
        Ok(StampedEntry::new(
            <V010ToV011 as VersionProjection<v010::Entry, Entry>>::project(self.entry.entry)
                .map_err(|error| Error::migration(error.to_string()))?,
            self.entry.date,
            self.entry.time,
        ))
    }
}

impl EngineRecord for V010StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}

fn migration_configuration_argument_text(argument: signal_frame::SingleArgument) -> Result<String> {
    let value = argument.as_str();
    if value.starts_with('(') {
        Ok(value.to_string())
    } else {
        fs::read_to_string(value).map_err(Error::input_output)
    }
}
