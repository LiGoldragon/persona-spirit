use std::{fs, path::PathBuf};

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use sema::SchemaVersion;
use sema_engine::{
    Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
};
use signal_persona_spirit::{
    Date, Entry, RecordIdentifier, Time,
    migration::{V010ToV011, V020ToV030, V030ToV040, v010, v020, v030},
};
use version_projection::VersionProjection;

use crate::{
    Error, Result, StoreLocation, StorePath,
    store::{SpiritStore, StampedEntry},
};

const V010_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const V020_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const V030_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const V040_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(4);
const V050_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(5);
const RECORDS: TableName = TableName::new("records");
const IDENTIFIER_MIGRATION_TABLE_EXTENSION: &str = "identifier-migration.nota";
const SHORT_IDENTIFIER_MIGRATION_TABLE_EXTENSION: &str = "short-identifier-migration.nota";
const SHORT_IDENTIFIER_MAXIMUM_CODE_LENGTH: usize = 7;

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

#[derive(Debug, Clone, PartialEq, Eq, NotaRecord)]
pub struct IdentifierMigrationTable {
    pub rows: Vec<IdentifierMigrationRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, NotaRecord)]
pub struct IdentifierMigrationRow {
    pub hash_identifier: RecordIdentifier,
    pub ordinal_identifier: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaRecord)]
pub struct ShortIdentifierMigrationTable {
    pub rows: Vec<ShortIdentifierMigrationRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, NotaRecord)]
pub struct ShortIdentifierMigrationRow {
    pub previous_identifier: RecordIdentifier,
    pub current_identifier: RecordIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentifierMigrationTablePath {
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShortIdentifierMigrationTablePath {
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct LegacyRecordIdentifier(u64);

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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V020StoredRecord {
    identifier: RecordIdentifier,
    entry: V020StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V020StampedEntry {
    entry: v020::Entry,
    date: Date,
    time: Time,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V030StoredRecord {
    identifier: RecordIdentifier,
    entry: V030StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V030StampedEntry {
    entry: v030::Entry,
    date: Date,
    time: Time,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V040StoredRecord {
    identifier: LegacyRecordIdentifier,
    entry: StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V050StoredRecord {
    identifier: RecordIdentifier,
    entry: StampedEntry,
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

    pub fn migrate_v020_to_next(self) -> Result<MigrationOutcome> {
        migrate_v020_to_next(&self.source, &self.target)
    }

    pub fn migrate_v030_to_v040(self) -> Result<MigrationOutcome> {
        migrate_v030_to_v040(&self.source, &self.target)
    }

    pub fn migrate_v040_to_v050(self) -> Result<MigrationOutcome> {
        V040ToV050Migration::new(&self.source, &self.target).migrate()
    }

    pub fn migrate_v050_to_v052(self) -> Result<MigrationOutcome> {
        V050ToV052Migration::new(&self.source, &self.target).migrate()
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

impl IdentifierMigrationTable {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn push(&mut self, row: IdentifierMigrationRow) {
        self.rows.push(row);
    }
}

impl IdentifierMigrationRow {
    const fn new(hash_identifier: RecordIdentifier, ordinal_identifier: u64) -> Self {
        Self {
            hash_identifier,
            ordinal_identifier,
        }
    }
}

impl ShortIdentifierMigrationTable {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn push(&mut self, row: ShortIdentifierMigrationRow) {
        self.rows.push(row);
    }
}

impl ShortIdentifierMigrationRow {
    const fn new(
        previous_identifier: RecordIdentifier,
        current_identifier: RecordIdentifier,
    ) -> Self {
        Self {
            previous_identifier,
            current_identifier,
        }
    }
}

impl IdentifierMigrationTablePath {
    fn from_target(target: &StorePath) -> Self {
        let mut path = target.as_path().to_path_buf();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("{name}.{IDENTIFIER_MIGRATION_TABLE_EXTENSION}"))
            .unwrap_or_else(|| format!("spirit.{IDENTIFIER_MIGRATION_TABLE_EXTENSION}"));
        path.set_file_name(file_name);
        Self { path }
    }

    fn write(&self, table: &IdentifierMigrationTable) -> Result<()> {
        let mut encoder = Encoder::new();
        table
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        fs::write(&self.path, encoder.into_string()).map_err(Error::input_output)
    }
}

impl ShortIdentifierMigrationTablePath {
    fn from_target(target: &StorePath) -> Self {
        let mut path = target.as_path().to_path_buf();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("{name}.{SHORT_IDENTIFIER_MIGRATION_TABLE_EXTENSION}"))
            .unwrap_or_else(|| format!("spirit.{SHORT_IDENTIFIER_MIGRATION_TABLE_EXTENSION}"));
        path.set_file_name(file_name);
        Self { path }
    }

    fn write(&self, table: &ShortIdentifierMigrationTable) -> Result<()> {
        let mut encoder = Encoder::new();
        table
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        fs::write(&self.path, encoder.into_string()).map_err(Error::input_output)
    }
}

impl LegacyRecordIdentifier {
    const fn value(self) -> u64 {
        self.0
    }

    fn record_key(self) -> RecordKey {
        RecordKey::new(self.value().to_string())
    }
}

struct V040ToV050Migration<'configuration> {
    source: &'configuration StorePath,
    target: &'configuration StorePath,
}

struct V050ToV052Migration<'configuration> {
    source: &'configuration StorePath,
    target: &'configuration StorePath,
}

impl<'configuration> V040ToV050Migration<'configuration> {
    const fn new(source: &'configuration StorePath, target: &'configuration StorePath) -> Self {
        Self { source, target }
    }

    fn migrate(self) -> Result<MigrationOutcome> {
        let source_records = V040Store::open(self.source)?.all_records()?;
        let target_store = SpiritStore::open(&StoreLocation::new(self.target.as_path()))?;
        if !target_store.is_empty()? {
            return Err(Error::migration(
                "target v0.5 database must be empty before identifier migration",
            ));
        }

        let mut table = IdentifierMigrationTable::new();
        let mut migrated = 0;
        for record in source_records {
            let ordinal_identifier = record.identifier.value();
            let accepted = target_store.assert_entry(record.project())?;
            table.push(IdentifierMigrationRow::new(
                accepted.identifier(),
                ordinal_identifier,
            ));
            migrated += 1;
        }
        IdentifierMigrationTablePath::from_target(self.target).write(&table)?;
        Ok(MigrationOutcome::new(migrated))
    }
}

impl<'configuration> V050ToV052Migration<'configuration> {
    const fn new(source: &'configuration StorePath, target: &'configuration StorePath) -> Self {
        Self { source, target }
    }

    fn migrate(self) -> Result<MigrationOutcome> {
        let source_records = V050Store::open(self.source)?.all_records()?;
        let target_store = SpiritStore::open(&StoreLocation::new(self.target.as_path()))?;
        if !target_store.is_empty()? {
            return Err(Error::migration(
                "target v0.5.2 database must be empty before short identifier migration",
            ));
        }

        let mut table = ShortIdentifierMigrationTable::new();
        let mut migrated = 0;
        for record in V050RecordGroups::from_records(source_records) {
            let previous_identifier = record.identifier;
            let current_identifier = if V050IdentifierPolicy::keeps_identifier(previous_identifier)
            {
                target_store.import_migrated_record(previous_identifier, record.entry)?;
                previous_identifier
            } else {
                target_store.assert_entry(record.entry)?.identifier()
            };
            table.push(ShortIdentifierMigrationRow::new(
                previous_identifier,
                current_identifier,
            ));
            migrated += 1;
        }
        ShortIdentifierMigrationTablePath::from_target(self.target).write(&table)?;
        Ok(MigrationOutcome::new(migrated))
    }
}

pub fn migrate_v010_to_v020(source: &StorePath, target: &StorePath) -> Result<MigrationOutcome> {
    let source_records = V010Store::open(source)?.all_records()?;
    let target_store = SpiritStore::open(&StoreLocation::new(target.as_path()))?;
    if !target_store.is_empty()? {
        return Err(Error::migration(
            "target database must be empty before timestamp-preserving migration",
        ));
    }

    let mut migrated = 0;
    for record in source_records {
        target_store.import_migrated_record(record.identifier, record.project()?)?;
        migrated += 1;
    }
    Ok(MigrationOutcome::new(migrated))
}

pub fn migrate_v020_to_next(source: &StorePath, target: &StorePath) -> Result<MigrationOutcome> {
    let source_records = V020Store::open(source)?.all_records()?;
    let target_store = SpiritStore::open(&StoreLocation::new(target.as_path()))?;
    if !target_store.is_empty()? {
        return Err(Error::migration(
            "target next database must be empty before multi-topic migration",
        ));
    }

    let mut migrated = 0;
    for record in source_records {
        target_store.import_migrated_record(record.identifier, record.project()?)?;
        migrated += 1;
    }
    Ok(MigrationOutcome::new(migrated))
}

pub fn migrate_v030_to_v040(source: &StorePath, target: &StorePath) -> Result<MigrationOutcome> {
    let source_records = V030Store::open(source)?.all_records()?;
    let target_store = SpiritStore::open(&StoreLocation::new(target.as_path()))?;
    if !target_store.is_empty()? {
        return Err(Error::migration(
            "target v0.4 database must be empty before privacy migration",
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

struct V020Store {
    engine: Engine,
    records: sema_engine::TableReference<V020StoredRecord>,
}

struct V030Store {
    engine: Engine,
    records: sema_engine::TableReference<V030StoredRecord>,
}

struct V040Store {
    engine: Engine,
    records: sema_engine::TableReference<V040StoredRecord>,
}

struct V050Store {
    engine: Engine,
    records: sema_engine::TableReference<V050StoredRecord>,
}

struct V050RecordGroups {
    short_records: Vec<V050StoredRecord>,
    long_records: Vec<V050StoredRecord>,
}

struct V050IdentifierPolicy;

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

impl V020Store {
    fn open(path: &StorePath) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(path.as_path(), V020_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn all_records(&self) -> Result<Vec<V020StoredRecord>> {
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

impl V030Store {
    fn open(path: &StorePath) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(path.as_path(), V030_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn all_records(&self) -> Result<Vec<V030StoredRecord>> {
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

impl V040Store {
    fn open(path: &StorePath) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(path.as_path(), V040_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn all_records(&self) -> Result<Vec<V040StoredRecord>> {
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

impl V050Store {
    fn open(path: &StorePath) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(path.as_path(), V050_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn all_records(&self) -> Result<Vec<V050StoredRecord>> {
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

impl V050RecordGroups {
    fn from_records(records: Vec<V050StoredRecord>) -> Self {
        let mut short_records = Vec::new();
        let mut long_records = Vec::new();
        for record in records {
            if V050IdentifierPolicy::keeps_identifier(record.identifier) {
                short_records.push(record);
            } else {
                long_records.push(record);
            }
        }
        Self {
            short_records,
            long_records,
        }
    }
}

impl IntoIterator for V050RecordGroups {
    type Item = V050StoredRecord;
    type IntoIter = std::iter::Chain<
        std::vec::IntoIter<V050StoredRecord>,
        std::vec::IntoIter<V050StoredRecord>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.short_records.into_iter().chain(self.long_records)
    }
}

impl V050IdentifierPolicy {
    fn keeps_identifier(identifier: RecordIdentifier) -> bool {
        identifier.code().len() <= SHORT_IDENTIFIER_MAXIMUM_CODE_LENGTH
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

impl V020StoredRecord {
    fn project(self) -> Result<StampedEntry> {
        Ok(StampedEntry::new(
            <V020ToV030 as VersionProjection<v020::Entry, Entry>>::project(self.entry.entry)
                .map_err(|error| Error::migration(error.to_string()))?,
            self.entry.date,
            self.entry.time,
        ))
    }
}

impl V030StoredRecord {
    fn project(self) -> Result<StampedEntry> {
        Ok(StampedEntry::new(
            <V030ToV040 as VersionProjection<v030::Entry, Entry>>::project(self.entry.entry)
                .map_err(|error| Error::migration(error.to_string()))?,
            self.entry.date,
            self.entry.time,
        ))
    }
}

impl V040StoredRecord {
    fn project(self) -> StampedEntry {
        self.entry
    }
}

impl EngineRecord for V010StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}

impl EngineRecord for V020StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}

impl EngineRecord for V030StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}

impl EngineRecord for V040StoredRecord {
    fn record_key(&self) -> RecordKey {
        self.identifier.record_key()
    }
}

impl EngineRecord for V050StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.code())
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
