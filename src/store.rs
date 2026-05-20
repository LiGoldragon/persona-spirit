use std::path::{Path, PathBuf};

use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
    TableReference,
};
use signal_persona_spirit::{
    Date, Entry, ObservationMode, Quote, RecordAccepted, RecordIdentifier, RecordObservation,
    RecordProvenance, RecordProvenancesObserved, RecordSummary, RecordsObserved, SpiritReply, Time,
    Topic,
};

use crate::{Result, error::Error};

const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const RECORDS: TableName = TableName::new("records");
const DEFAULT_STORE_PATH: &str = "/tmp/persona-spirit.redb";
const STORE_ENVIRONMENT_VARIABLE: &str = "PERSONA_SPIRIT_STORE";
const STATE_ENVIRONMENT_VARIABLE: &str = "PERSONA_STATE_PATH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    path: PathBuf,
}

pub struct SpiritStore {
    engine: Engine,
    records: TableReference<StoredRecord>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRecord {
    identifier: RecordIdentifier,
    entry: StampedEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordIdentifierMint {
    next: u64,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StampedEntry {
    entry: Entry,
    date: Date,
    time: Time,
}

impl StoreLocation {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn from_environment() -> Self {
        if let Some(path) = std::env::var_os(STORE_ENVIRONMENT_VARIABLE) {
            return Self::new(path);
        }
        if let Some(path) = std::env::var_os(STATE_ENVIRONMENT_VARIABLE) {
            return Self::new(path);
        }
        Self::new(DEFAULT_STORE_PATH)
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }
}

impl SpiritStore {
    pub fn open(location: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(location.as_path(), SPIRIT_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    pub fn assert_entry(&self, entry: StampedEntry) -> Result<RecordAccepted> {
        let stored = StoredRecord::new(self.next_identifier()?, entry);
        self.engine
            .assert(Assertion::new(self.records, stored.clone()))
            .map_err(Error::spirit_store)?;
        Ok(RecordAccepted {
            captured: stored.summary(),
        })
    }

    pub fn observe_records(&self, observation: RecordObservation) -> Result<SpiritReply> {
        let records = self.records_for_topic(observation.query.topic.as_ref())?;
        match observation.query.mode {
            ObservationMode::SummaryOnly => Ok(SpiritReply::RecordsObserved(RecordsObserved {
                records: records.iter().map(StoredRecord::summary).collect(),
            })),
            ObservationMode::WithProvenance => Ok(SpiritReply::RecordProvenancesObserved(
                RecordProvenancesObserved {
                    records: records.into_iter().map(StoredRecord::provenance).collect(),
                },
            )),
        }
    }

    pub fn summaries_for_topic(&self, topic: Option<&Topic>) -> Result<Vec<RecordSummary>> {
        Ok(self
            .records_for_topic(topic)?
            .iter()
            .map(StoredRecord::summary)
            .collect())
    }

    fn next_identifier(&self) -> Result<RecordIdentifier> {
        Ok(RecordIdentifierMint::from_records(&self.all_records()?).next_identifier())
    }

    fn records_for_topic(&self, topic: Option<&Topic>) -> Result<Vec<StoredRecord>> {
        Ok(self
            .all_records()?
            .into_iter()
            .filter(|record| {
                topic
                    .map(|expected| &record.entry.entry.topic == expected)
                    .unwrap_or(true)
            })
            .collect())
    }

    fn all_records(&self) -> Result<Vec<StoredRecord>> {
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

impl StoredRecord {
    fn new(identifier: RecordIdentifier, entry: StampedEntry) -> Self {
        Self { identifier, entry }
    }

    fn summary(&self) -> RecordSummary {
        RecordSummary {
            identifier: self.identifier,
            topic: self.entry.entry.topic.clone(),
            kind: self.entry.entry.kind,
            summary: self.entry.entry.summary.clone(),
            certainty: self.entry.entry.certainty,
        }
    }

    fn provenance(self) -> RecordProvenance {
        RecordProvenance {
            summary: self.summary(),
            context: self.entry.entry.context,
            date: self.entry.date,
            time: self.entry.time,
            quote: self.entry.entry.quote,
        }
    }
}

impl StampedEntry {
    pub fn new(entry: Entry, date: Date, time: Time) -> Self {
        Self { entry, date, time }
    }

    pub fn quote(&self) -> &Quote {
        &self.entry.quote
    }
}

impl EngineRecord for StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}

impl RecordIdentifierMint {
    fn from_records(records: &[StoredRecord]) -> Self {
        let next = records
            .iter()
            .map(|record| record.identifier.value())
            .max()
            .unwrap_or(0)
            + 1;
        Self { next }
    }

    fn next_identifier(&self) -> RecordIdentifier {
        RecordIdentifier::new(self.next)
    }
}
