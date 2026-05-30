use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, Retraction,
    TableDescriptor, TableName, TableReference,
};
use signal_persona_spirit::{
    Certainty, CertaintyChange, CertaintyChanged, CertaintySelection, Date, Entry, Kind,
    ObservationMode, RecordAccepted, RecordIdentifier, RecordIdentifierQuery, RecordObservation,
    RecordProvenance, RecordProvenancesObserved, RecordQuery, RecordRemoved, RecordSummary,
    RecordedTime, RecordedTimeSelection, RecordsObserved, Reply as WorkingReply, Time, Topic,
    TopicCount, TopicSelection, Topics, TopicsObserved,
};
use signal_version_handover::{HandoverMarker, MarkerRequest};
use version_projection::{ComponentName, ContractVersion, Projected};

use crate::{Result, error::Error};

const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const SPIRIT_CONTRACT_VERSION: ContractVersion = ContractVersion::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
]);
const RECORDS: TableName = TableName::new("records");
const DEFAULT_STORE_PATH: &str = "/tmp/persona-spirit.redb";
const STORE_ENVIRONMENT_VARIABLE: &str = "PERSONA_SPIRIT_STORE";
const STATE_ENVIRONMENT_VARIABLE: &str = "PERSONA_STATE_PATH";
const RECENT_RECORD_LIMIT: usize = 20;

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
        Self::validate_topics(&entry.entry.topics)?;
        let stored = StoredRecord::new(self.next_identifier()?, entry);
        self.engine
            .assert(Assertion::new(self.records, stored.clone()))
            .map_err(Error::spirit_store)?;
        Ok(RecordAccepted::new(stored.identifier))
    }

    pub fn remove_entry(&self, identifier: RecordIdentifier) -> Result<RecordRemoved> {
        self.engine
            .retract(Retraction::new(self.records, StoredRecord::key(identifier)))
            .map_err(Error::spirit_store)?;
        Ok(RecordRemoved::new(identifier))
    }

    pub fn change_certainty(&self, change: CertaintyChange) -> Result<CertaintyChanged> {
        let stored = self
            .stored_record(change.identifier)?
            .with_certainty(change.certainty);
        self.engine
            .mutate(Mutation::new(self.records, stored))
            .map_err(Error::spirit_store)?;
        Ok(CertaintyChanged {
            identifier: change.identifier,
            certainty: change.certainty,
        })
    }

    pub(crate) fn import_migrated_record(
        &self,
        identifier: RecordIdentifier,
        entry: StampedEntry,
    ) -> Result<()> {
        Self::validate_topics(&entry.entry.topics)?;
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
        Ok(RecordReply::new(records, observation.query.mode).into_working_reply())
    }

    pub fn observe_record_identifiers(&self, query: RecordIdentifierQuery) -> Result<WorkingReply> {
        let records = self.records_for_identifier_query(query)?;
        Ok(RecordReply::new(records, query.mode).into_working_reply())
    }

    fn records_for_identifier_query(
        &self,
        query: RecordIdentifierQuery,
    ) -> Result<Vec<StoredRecord>> {
        Ok(self
            .all_records()?
            .into_iter()
            .filter(|record| query.contains(record.identifier))
            .collect())
    }

    fn records_for_query(&self, query: &RecordQuery) -> Result<Vec<StoredRecord>> {
        let mut records = self
            .all_records()?
            .into_iter()
            .filter(|record| RecordFilter::new(query).matches(record))
            .collect::<Vec<_>>();
        if query.recorded_time_selection == RecordedTimeSelection::Recent {
            RecentRecordSelection::new(RECENT_RECORD_LIMIT).retain(&mut records);
        }
        Ok(records)
    }

    pub fn observe_topics(&self) -> Result<WorkingReply> {
        Ok(WorkingReply::TopicsObserved(TopicsObserved::new(
            self.topic_counts()?,
        )))
    }

    pub fn summaries_for_topic(&self, topic: Option<&Topic>) -> Result<Vec<RecordSummary>> {
        let topic_selection = topic
            .cloned()
            .map(|topic| TopicSelection::partial(vec![topic]))
            .unwrap_or_else(TopicSelection::any);
        let query = RecordQuery {
            topic_selection,
            kind: None,
            certainty_selection: CertaintySelection::Any,
            recorded_time_selection: RecordedTimeSelection::Any,
            mode: ObservationMode::SummaryOnly,
        };
        Ok(self
            .records_for_query(&query)?
            .iter()
            .map(StoredRecord::summary)
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
        let records = self.all_records()?;
        let commit_sequence = self
            .engine
            .current_commit_sequence()
            .map_err(Error::spirit_store)?
            .value();
        Ok(
            RecordIdentifierMint::from_records_and_commit_sequence(&records, commit_sequence)
                .next_identifier(),
        )
    }

    fn last_record_identifier(&self) -> Result<Option<u64>> {
        Ok(self
            .all_records()?
            .last()
            .map(|record| record.identifier.value()))
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

    fn stored_record(&self, identifier: RecordIdentifier) -> Result<StoredRecord> {
        self.all_records()?
            .into_iter()
            .find(|record| record.identifier == identifier)
            .ok_or_else(|| Error::RequestRejected {
                reason: format!(
                    "record is not stored: {}/{}",
                    RECORDS.as_str(),
                    identifier.value()
                ),
            })
    }

    fn topic_counts(&self) -> Result<Vec<TopicCount>> {
        let mut counts = BTreeMap::<String, u64>::new();
        for record in self.all_records()? {
            for topic in record.entry.entry.topics.as_slice() {
                *counts.entry(topic.as_str().to_owned()).or_insert(0) += 1;
            }
        }
        Ok(counts
            .into_iter()
            .map(|(topic, entries)| TopicCount {
                topic: Topic::new(topic),
                entries,
            })
            .collect())
    }

    fn validate_topics(topics: &Topics) -> Result<()> {
        if topics.is_empty() {
            return Err(Error::RequestRejected {
                reason: "record must carry at least one topic".to_string(),
            });
        }
        let mut seen = std::collections::BTreeSet::<&str>::new();
        for topic in topics.as_slice() {
            if !seen.insert(topic.as_str()) {
                return Err(Error::RequestRejected {
                    reason: format!("record repeats topic {}", topic.as_str()),
                });
            }
        }
        Ok(())
    }
}

struct RecordReply {
    records: Vec<StoredRecord>,
    mode: ObservationMode,
}

impl RecordReply {
    fn new(records: Vec<StoredRecord>, mode: ObservationMode) -> Self {
        Self { records, mode }
    }

    fn into_working_reply(self) -> WorkingReply {
        let Self { records, mode } = self;
        match mode {
            ObservationMode::SummaryOnly => WorkingReply::RecordsObserved(RecordsObserved::new(
                records.iter().map(StoredRecord::summary).collect(),
            )),
            ObservationMode::WithProvenance => {
                WorkingReply::RecordProvenancesObserved(RecordProvenancesObserved::new(
                    records.into_iter().map(StoredRecord::provenance).collect(),
                ))
            }
        }
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
    topic_selection: &'query TopicSelection,
    kind: Option<Kind>,
    certainty_selection: CertaintySelection,
    recorded_time_selection: RecordedTimeSelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecentRecordSelection {
    maximum_records: usize,
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

    fn key(identifier: RecordIdentifier) -> RecordKey {
        RecordKey::new(identifier.value().to_string())
    }

    fn summary(&self) -> RecordSummary {
        RecordSummary {
            identifier: self.identifier,
            topics: self.entry.entry.topics.clone(),
            kind: self.entry.entry.kind,
            description: self.entry.entry.description.clone(),
            certainty: self.entry.entry.certainty,
        }
    }

    fn provenance(self) -> RecordProvenance {
        RecordProvenance {
            summary: self.summary(),
            date: self.entry.date,
            time: self.entry.time,
        }
    }

    fn recorded_time(&self) -> RecordedTime {
        RecordedTime::new(self.entry.date, self.entry.time)
    }

    fn with_certainty(mut self, certainty: Certainty) -> Self {
        self.entry.change_certainty(certainty);
        self
    }
}

impl RecentRecordSelection {
    const fn new(maximum_records: usize) -> Self {
        Self { maximum_records }
    }

    fn retain(self, records: &mut Vec<StoredRecord>) {
        records.sort_by_key(|record| (record.recorded_time(), record.identifier.value()));
        let overflow = records.len().saturating_sub(self.maximum_records);
        if overflow > 0 {
            records.drain(0..overflow);
        }
    }
}

impl<'query> RecordFilter<'query> {
    fn new(query: &'query RecordQuery) -> Self {
        Self {
            topic_selection: &query.topic_selection,
            kind: query.kind,
            certainty_selection: query.certainty_selection,
            recorded_time_selection: query.recorded_time_selection,
        }
    }

    fn matches(&self, record: &StoredRecord) -> bool {
        self.matches_topic(record)
            && self.matches_kind(record)
            && self.matches_certainty(record)
            && self.matches_recorded_time(record)
    }

    fn matches_topic(&self, record: &StoredRecord) -> bool {
        self.topic_selection.matches(&record.entry.entry.topics)
    }

    fn matches_kind(&self, record: &StoredRecord) -> bool {
        self.kind
            .map(|expected| record.entry.entry.kind == expected)
            .unwrap_or(true)
    }

    fn matches_certainty(&self, record: &StoredRecord) -> bool {
        self.certainty_selection
            .matches(record.entry.entry.certainty)
    }

    fn matches_recorded_time(&self, record: &StoredRecord) -> bool {
        self.recorded_time_selection.matches(record.recorded_time())
    }
}

impl StampedEntry {
    pub fn new(entry: Entry, date: Date, time: Time) -> Self {
        Self { entry, date, time }
    }

    fn change_certainty(&mut self, certainty: Certainty) {
        self.entry.certainty = certainty;
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
        Self::key(self.identifier)
    }
}

impl RecordIdentifierMint {
    fn from_records_and_commit_sequence(records: &[StoredRecord], commit_sequence: u64) -> Self {
        let last_record_identifier = records
            .iter()
            .map(|record| record.identifier.value())
            .max()
            .unwrap_or(0);
        let next = last_record_identifier.max(commit_sequence) + 1;
        Self { next }
    }

    fn next_identifier(&self) -> RecordIdentifier {
        RecordIdentifier::new(self.next)
    }
}

#[cfg(test)]
mod tests {
    use signal_persona_spirit::{Description, Kind, RecordedTimeRange};
    use signal_sema::Magnitude;

    use super::*;

    #[derive(Debug, Clone)]
    struct StoreFixture {
        location: StoreLocation,
    }

    impl StoreFixture {
        fn new(test_name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos();
            let mut path = std::env::temp_dir();
            path.push(format!("persona-spirit-store-{test_name}-{nanos}.redb"));
            Self {
                location: StoreLocation::new(path),
            }
        }

        fn store(&self) -> SpiritStore {
            SpiritStore::open(&self.location).expect("store opens")
        }
    }

    #[test]
    fn stamped_entry_composes_entry_with_daemon_date_and_time() {
        let entry = Entry {
            topics: Topics::single(Topic::new("workspace")),
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

    #[test]
    fn record_query_filters_by_recorded_time_range_after_topic_match() {
        let fixture = StoreFixture::new("time-range");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("outside early"),
                    certainty: Magnitude::Maximum,
                },
                Date::new(2026, 5, 28),
                Time::new(23, 59, 59),
            ))
            .expect("early record accepted");
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("inside"),
                    certainty: Magnitude::Maximum,
                },
                Date::new(2026, 5, 29),
                Time::new(12, 0, 0),
            ))
            .expect("inside record accepted");
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("other")),
                    kind: Kind::Decision,
                    description: Description::new("matching time wrong topic"),
                    certainty: Magnitude::Maximum,
                },
                Date::new(2026, 5, 29),
                Time::new(13, 0, 0),
            ))
            .expect("other topic record accepted");

        let reply = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Between(
                        RecordedTimeRange::new(
                            RecordedTime::new(Date::new(2026, 5, 29), Time::new(0, 0, 0)),
                            RecordedTime::new(Date::new(2026, 5, 29), Time::new(23, 59, 59)),
                        ),
                    ),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("records observed");

        let WorkingReply::RecordProvenancesObserved(records) = reply else {
            panic!("expected provenances");
        };
        let records = records.into_records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].summary.description, Description::new("inside"));
    }

    #[test]
    fn recent_record_query_keeps_newest_records_after_other_filters() {
        let fixture = StoreFixture::new("recent");
        let store = fixture.store();
        for day in 1..=25 {
            store
                .assert_entry(StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("spirit")),
                        kind: Kind::Decision,
                        description: Description::new(format!("spirit day {day}")),
                        certainty: Magnitude::Maximum,
                    },
                    Date::new(2026, 5, day),
                    Time::new(12, 0, 0),
                ))
                .expect("spirit record accepted");
            store
                .assert_entry(StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("other")),
                        kind: Kind::Decision,
                        description: Description::new(format!("other day {day}")),
                        certainty: Magnitude::Maximum,
                    },
                    Date::new(2026, 5, day),
                    Time::new(12, 30, 0),
                ))
                .expect("other record accepted");
        }

        let reply = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Recent,
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("records observed");

        let WorkingReply::RecordProvenancesObserved(records) = reply else {
            panic!("expected provenances");
        };
        let records = records.into_records();
        assert_eq!(records.len(), RECENT_RECORD_LIMIT);
        assert_eq!(records[0].date, Date::new(2026, 5, 6));
        assert_eq!(records[19].date, Date::new(2026, 5, 25));
        assert!(
            records
                .iter()
                .all(|record| record.summary.topics.contains(&Topic::new("spirit")))
        );
    }
}
