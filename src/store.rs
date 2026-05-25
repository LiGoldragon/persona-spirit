use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, QueryPlan, RecordKey, TableDescriptor, TableName,
    TableReference,
};
use signal_persona_spirit::{
    Date, Entry, Kind, ObservationMode, RecordAccepted, RecordDescription, RecordIdentifier,
    RecordObservation, RecordProvenance, RecordProvenancesObserved, RecordQuery, RecordsObserved,
    Reply as WorkingReply, Time, Topic, TopicCount, TopicsObserved,
};
use signal_version_handover::{HandoverMarker, MarkerRequest};
use version_projection::{ComponentName, ContractVersion, Projected};

use crate::{Result, error::Error};

const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const SPIRIT_CONTRACT_VERSION: ContractVersion = ContractVersion::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
]);
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
        Ok(RecordAccepted::new(stored.identifier))
    }

    pub(crate) fn import_migrated_record(
        &self,
        identifier: RecordIdentifier,
        entry: StampedEntry,
    ) -> Result<()> {
        self.engine
            .assert(Assertion::new(
                self.records,
                StoredRecord::new(identifier, entry),
            ))
            .map_err(Error::spirit_store)?;
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.all_records()?.is_empty())
    }

    pub fn observe_records(&self, observation: RecordObservation) -> Result<WorkingReply> {
        let records = self.records_for_query(&observation.query)?;
        match observation.query.mode {
            ObservationMode::DescriptionOnly => {
                Ok(WorkingReply::RecordsObserved(RecordsObserved {
                    records: records.iter().map(StoredRecord::description).collect(),
                }))
            }
            ObservationMode::WithProvenance => Ok(WorkingReply::RecordProvenancesObserved(
                RecordProvenancesObserved {
                    records: records.into_iter().map(StoredRecord::provenance).collect(),
                },
            )),
        }
    }

    pub fn observe_topics(&self) -> Result<WorkingReply> {
        Ok(WorkingReply::TopicsObserved(TopicsObserved {
            topics: self.topic_counts()?,
        }))
    }

    pub fn descriptions_for_topic(&self, topic: Option<&Topic>) -> Result<Vec<RecordDescription>> {
        let query = RecordQuery {
            topic: topic.cloned(),
            kind: None,
            mode: ObservationMode::DescriptionOnly,
        };
        Ok(self
            .records_for_query(&query)?
            .iter()
            .map(StoredRecord::description)
            .collect())
    }

    pub fn handover_marker(
        &self,
        request: MarkerRequest,
        schema_hash: ContractVersion,
    ) -> Result<HandoverMarker> {
        let reading = HandoverClock::read();
        let commit_sequence = self
            .engine
            .current_commit_sequence()
            .map_err(Error::spirit_store)?
            .value();
        Ok(HandoverMarker {
            component: request.component,
            schema_hash,
            commit_sequence,
            write_counter: commit_sequence,
            last_record_identifier: self.last_record_identifier()?,
            recorded_at_date: reading.date,
            recorded_at_time: reading.time,
        })
    }

    fn next_identifier(&self) -> Result<RecordIdentifier> {
        Ok(RecordIdentifierMint::from_records(&self.all_records()?).next_identifier())
    }

    fn last_record_identifier(&self) -> Result<Option<u64>> {
        Ok(self
            .all_records()?
            .last()
            .map(|record| record.identifier.value()))
    }

    fn records_for_query(&self, query: &RecordQuery) -> Result<Vec<StoredRecord>> {
        Ok(self
            .all_records()?
            .into_iter()
            .filter(|record| RecordFilter::new(query).matches(record))
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

    fn topic_counts(&self) -> Result<Vec<TopicCount>> {
        let mut counts = BTreeMap::<String, u64>::new();
        for record in self.all_records()? {
            *counts
                .entry(record.entry.entry.topic.as_str().to_owned())
                .or_insert(0) += 1;
        }
        Ok(counts
            .into_iter()
            .map(|(topic, entries)| TopicCount {
                topic: Topic::new(topic),
                entries,
            })
            .collect())
    }
}

pub const fn spirit_contract_version() -> ContractVersion {
    SPIRIT_CONTRACT_VERSION
}

struct HandoverClock;

struct HandoverClockReading {
    date: signal_version_handover::Date,
    time: signal_version_handover::Time,
}

struct RecordFilter<'query> {
    topic: Option<&'query Topic>,
    kind: Option<Kind>,
}

impl HandoverClock {
    fn read() -> HandoverClockReading {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        HandoverClockReading::from_unix_seconds(seconds)
    }
}

impl HandoverClockReading {
    fn from_unix_seconds(seconds: u64) -> Self {
        let days = (seconds / 86_400) as i64;
        let seconds_of_day = seconds % 86_400;
        let (year, month, day) = HandoverCivilDate::from_unix_days(days).into_parts();
        Self {
            date: signal_version_handover::Date::new(year as u16, month as u8, day as u8),
            time: signal_version_handover::Time::new(
                (seconds_of_day / 3_600) as u8,
                ((seconds_of_day % 3_600) / 60) as u8,
                (seconds_of_day % 60) as u8,
            ),
        }
    }
}

struct HandoverCivilDate {
    year: i32,
    month: u32,
    day: u32,
}

impl HandoverCivilDate {
    fn from_unix_days(days: i64) -> Self {
        let zero_based_days = days + 719_468;
        let era = if zero_based_days >= 0 {
            zero_based_days
        } else {
            zero_based_days - 146_096
        } / 146_097;
        let day_of_era = zero_based_days - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_parameter = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_parameter + 2) / 5 + 1;
        let month = month_parameter + if month_parameter < 10 { 3 } else { -9 };
        if month <= 2 {
            year += 1;
        }
        Self {
            year: year as i32,
            month: month as u32,
            day: day as u32,
        }
    }

    fn into_parts(self) -> (i32, u32, u32) {
        (self.year, self.month, self.day)
    }
}

impl StoredRecord {
    fn new(identifier: RecordIdentifier, entry: StampedEntry) -> Self {
        Self { identifier, entry }
    }

    fn description(&self) -> RecordDescription {
        RecordDescription {
            identifier: self.identifier,
            topic: self.entry.entry.topic.clone(),
            kind: self.entry.entry.kind,
            description: self.entry.entry.description.clone(),
            certainty: self.entry.entry.certainty,
        }
    }

    fn provenance(self) -> RecordProvenance {
        RecordProvenance {
            description: self.description(),
            date: self.entry.date,
            time: self.entry.time,
        }
    }
}

impl<'query> RecordFilter<'query> {
    fn new(query: &'query RecordQuery) -> Self {
        Self {
            topic: query.topic.as_ref(),
            kind: query.kind,
        }
    }

    fn matches(&self, record: &StoredRecord) -> bool {
        self.matches_topic(record) && self.matches_kind(record)
    }

    fn matches_topic(&self, record: &StoredRecord) -> bool {
        self.topic
            .map(|expected| &record.entry.entry.topic == expected)
            .unwrap_or(true)
    }

    fn matches_kind(&self, record: &StoredRecord) -> bool {
        self.kind
            .map(|expected| record.entry.entry.kind == expected)
            .unwrap_or(true)
    }
}

impl StampedEntry {
    pub fn new(entry: Entry, date: Date, time: Time) -> Self {
        Self { entry, date, time }
    }
}

impl Projected for StampedEntry {
    const CONTRACT_VERSION: ContractVersion = SPIRIT_CONTRACT_VERSION;

    fn component() -> ComponentName {
        ComponentName::new("persona-spirit")
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

#[cfg(test)]
mod tests {
    use signal_persona_spirit::{Description, Kind};
    use signal_sema::Magnitude;

    use super::*;

    #[test]
    fn stamped_entry_composes_entry_with_daemon_date_and_time() {
        let entry = Entry {
            topic: Topic::new("workspace"),
            kind: Kind::Decision,
            description: Description::new("composition"),
            certainty: Magnitude::Maximum,
        };
        let date = Date::new(2026, 5, 21);
        let time = Time::new(10, 45, 0);

        let stamped = StampedEntry::new(entry.clone(), date, time);

        assert_eq!(stamped.entry, entry);
        assert_eq!(stamped.date, date);
        assert_eq!(stamped.time, time);
    }
}
