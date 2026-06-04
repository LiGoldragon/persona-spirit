use std::collections::BTreeMap;
use std::fs::File as FileSystemFile;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use nota_codec::{Encoder, NotaEncode};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey, Retraction,
    TableDescriptor, TableName, TableReference,
};
use signal_persona_spirit::{
    ArchivePath, ArchiveTarget, Certainty, CertaintyChange, CertaintyChanged, CertaintySelection,
    Date, Entry, Kind, ObservationMode, PrivacySelection, RecordAccepted, RecordIdentifier,
    RecordIdentifierQuery, RecordObservation, RecordProvenance, RecordProvenancesObserved,
    RecordQuery, RecordRemoved, RecordSummary, RecordedTime, RecordedTimeSelection,
    RecordsObserved, RemovalCandidateCollection, RemovalCandidateSkipReason,
    RemovalCandidatesCollected, Reply as WorkingReply, SkippedRemovalCandidate, Time, Topic,
    TopicCount, TopicSelection, Topics, TopicsObserved,
};
use signal_version_handover::{
    Date as HandoverDate, HandoverMarker, MarkerRequest, Time as HandoverTime,
};
use version_projection::{ComponentName, ContractVersion, Projected};

use crate::actors::clock::{CivilDate, CivilInstant, CivilTime};
use crate::{Result, error::Error};

const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const SPIRIT_CONTRACT_VERSION: ContractVersion = ContractVersion::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
]);
const RECORDS: TableName = TableName::new("records");
const DEFAULT_STORE_PATH: &str = "/tmp/persona-spirit.redb";
const STORE_ENVIRONMENT_VARIABLE: &str = "PERSONA_SPIRIT_STORE";
const STATE_ENVIRONMENT_VARIABLE: &str = "PERSONA_STATE_PATH";
const SHALLOW_RECORD_LIMIT: usize = 5;
const RECENT_RECORD_LIMIT: usize = 15;
const DEEP_RECORD_LIMIT: usize = 30;
const VERY_DEEP_RECORD_LIMIT: usize = 100;

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

    pub fn collect_removal_candidates(
        &self,
        collection: RemovalCandidateCollection,
    ) -> Result<RemovalCandidatesCollected> {
        CollectionQueryGuard::new(&collection).validate()?;
        let candidates = self.records_for_query(&collection.record_query)?;
        let archive = RemovalCandidateArchive::from_stored_records(&candidates);
        if archive.write_to_target(&collection.archive_target).is_err() {
            return Ok(RemovalCandidatesCollected::new(
                Vec::new(),
                Vec::new(),
                candidates
                    .iter()
                    .map(|record| SkippedRemovalCandidate {
                        identifier: record.identifier,
                        reason: RemovalCandidateSkipReason::ArchiveFailed,
                    })
                    .collect(),
            ));
        }
        for record in &candidates {
            self.engine
                .retract(Retraction::new(
                    self.records,
                    StoredRecord::key(record.identifier),
                ))
                .map_err(Error::spirit_store)?;
        }
        Ok(RemovalCandidatesCollected::new(
            archive.records(),
            candidates.iter().map(|record| record.identifier).collect(),
            Vec::new(),
        ))
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
        if let Some(selection) =
            RecentRecordSelection::from_recorded_time_selection(query.recorded_time_selection)
        {
            selection.retain(&mut records);
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
            privacy_selection: PrivacySelection::default_observation_privacy(),
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
        let reading = HandoverClockReading::from_now();
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

struct CollectionQueryGuard<'collection> {
    collection: &'collection RemovalCandidateCollection,
}

struct RemovalCandidateArchive {
    records: Vec<RecordSummary>,
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

impl<'collection> CollectionQueryGuard<'collection> {
    fn new(collection: &'collection RemovalCandidateCollection) -> Self {
        Self { collection }
    }

    fn validate(&self) -> Result<()> {
        if self.collection.is_exact_zero_candidate_query() {
            return Ok(());
        }
        Err(Error::RequestRejected {
            reason: "CollectRemovalCandidates requires an exact Zero certainty query".to_string(),
        })
    }
}

impl RemovalCandidateArchive {
    fn from_stored_records(records: &[StoredRecord]) -> Self {
        Self {
            records: records.iter().map(StoredRecord::summary).collect(),
        }
    }

    fn records(&self) -> Vec<RecordSummary> {
        self.records.clone()
    }

    fn write_to_target(&self, target: &ArchiveTarget) -> Result<()> {
        match target {
            ArchiveTarget::Inline => Ok(()),
            ArchiveTarget::File(path) => self.write_to_file(path),
        }
    }

    fn write_to_file(&self, path: &ArchivePath) -> Result<()> {
        let observed = WorkingReply::RecordsObserved(RecordsObserved::new(self.records.clone()));
        let mut encoder = Encoder::new();
        observed
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        let mut file = FileSystemFile::create(path.as_str()).map_err(Error::archive_write)?;
        file.write_all(encoder.into_string().as_bytes())
            .map_err(Error::archive_write)?;
        file.write_all(b"\n").map_err(Error::archive_write)?;
        file.sync_all().map_err(Error::archive_write)?;
        Ok(())
    }
}

pub const fn spirit_contract_version() -> ContractVersion {
    SPIRIT_CONTRACT_VERSION
}

struct HandoverClockReading {
    date: HandoverDate,
    time: HandoverTime,
}

struct RecordFilter<'query> {
    topic_selection: &'query TopicSelection,
    kind: Option<Kind>,
    certainty_selection: CertaintySelection,
    recorded_time_selection: RecordedTimeSelection,
    privacy_selection: PrivacySelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecentRecordSelection {
    maximum_records: usize,
}

impl HandoverClockReading {
    fn from_now() -> Self {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let instant = CivilInstant::from_unix_seconds(seconds);
        Self {
            date: instant.date.into(),
            time: instant.time.into(),
        }
    }
}

impl From<CivilDate> for HandoverDate {
    fn from(date: CivilDate) -> Self {
        Self::new(date.year() as u16, date.month() as u8, date.day() as u8)
    }
}

impl From<CivilTime> for HandoverTime {
    fn from(time: CivilTime) -> Self {
        Self::new(time.hour(), time.minute(), time.second())
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
            privacy: self.entry.entry.privacy,
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

    const fn from_recorded_time_selection(selection: RecordedTimeSelection) -> Option<Self> {
        match selection {
            RecordedTimeSelection::Shallow => Some(Self::new(SHALLOW_RECORD_LIMIT)),
            RecordedTimeSelection::Recent => Some(Self::new(RECENT_RECORD_LIMIT)),
            RecordedTimeSelection::Deep => Some(Self::new(DEEP_RECORD_LIMIT)),
            RecordedTimeSelection::VeryDeep => Some(Self::new(VERY_DEEP_RECORD_LIMIT)),
            _ => None,
        }
    }

    fn retain(self, records: &mut Vec<StoredRecord>) {
        records.sort_by_key(|record| {
            std::cmp::Reverse((record.recorded_time(), record.identifier.value()))
        });
        records.truncate(self.maximum_records);
    }
}

impl<'query> RecordFilter<'query> {
    fn new(query: &'query RecordQuery) -> Self {
        Self {
            topic_selection: &query.topic_selection,
            kind: query.kind,
            certainty_selection: query.certainty_selection,
            recorded_time_selection: query.recorded_time_selection,
            privacy_selection: query.privacy_selection,
        }
    }

    fn matches(&self, record: &StoredRecord) -> bool {
        self.matches_topic(record)
            && self.matches_kind(record)
            && self.matches_certainty(record)
            && self.matches_recorded_time(record)
            && self.matches_privacy(record)
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

    fn matches_privacy(&self, record: &StoredRecord) -> bool {
        self.privacy_selection.matches(record.entry.entry.privacy)
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
    use signal_persona_spirit::{
        ArchiveTarget, Description, Kind, RecordedTimeRange, RemovalCandidateCollection,
    };
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
            privacy: Magnitude::Zero,
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
                    privacy: Magnitude::Zero,
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
                    privacy: Magnitude::Zero,
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
                    privacy: Magnitude::Zero,
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
                    privacy_selection: PrivacySelection::default_observation_privacy(),
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
                        privacy: Magnitude::Zero,
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
                        privacy: Magnitude::Zero,
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
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("records observed");

        let WorkingReply::RecordProvenancesObserved(records) = reply else {
            panic!("expected provenances");
        };
        let records = records.into_records();
        assert_eq!(records.len(), RECENT_RECORD_LIMIT);
        assert_eq!(records[0].date, Date::new(2026, 5, 25));
        assert_eq!(records[14].date, Date::new(2026, 5, 11));
        assert!(
            records
                .iter()
                .all(|record| record.summary.topics.contains(&Topic::new("spirit")))
        );
    }

    #[test]
    fn qualitative_depth_queries_keep_newest_records_at_larger_depths() {
        let fixture = StoreFixture::new("qualitative-depth");
        let store = fixture.store();
        for index in 1..=80 {
            let month = 5 + ((index - 1) / 28) as u8;
            let day = 1 + ((index - 1) % 28) as u8;
            store
                .assert_entry(StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("spirit")),
                        kind: Kind::Decision,
                        description: Description::new(format!("spirit item {index}")),
                        certainty: Magnitude::Maximum,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, month, day),
                    Time::new(12, 0, 0),
                ))
                .expect("spirit record accepted");
        }

        let shallow = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Shallow,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("shallow records observed");
        let recent = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Recent,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("recent records observed");
        let deep = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Deep,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("deep records observed");
        let very_deep = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::partial(vec![Topic::new("spirit")]),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::VeryDeep,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::WithProvenance,
                },
            })
            .expect("very deep records observed");

        let WorkingReply::RecordProvenancesObserved(shallow) = shallow else {
            panic!("expected shallow provenances");
        };
        let WorkingReply::RecordProvenancesObserved(recent) = recent else {
            panic!("expected recent provenances");
        };
        let WorkingReply::RecordProvenancesObserved(deep) = deep else {
            panic!("expected deep provenances");
        };
        let WorkingReply::RecordProvenancesObserved(very_deep) = very_deep else {
            panic!("expected very deep provenances");
        };
        let shallow = shallow.into_records();
        let recent = recent.into_records();
        let deep = deep.into_records();
        let very_deep = very_deep.into_records();
        assert_eq!(shallow.len(), SHALLOW_RECORD_LIMIT);
        assert_eq!(recent.len(), RECENT_RECORD_LIMIT);
        assert_eq!(deep.len(), DEEP_RECORD_LIMIT);
        assert_eq!(very_deep.len(), 80);
        assert_eq!(
            shallow[0].summary.description,
            Description::new("spirit item 80")
        );
        assert_eq!(
            recent[0].summary.description,
            Description::new("spirit item 80")
        );
        assert_eq!(
            deep[0].summary.description,
            Description::new("spirit item 80")
        );
        assert_eq!(
            very_deep[79].summary.description,
            Description::new("spirit item 1")
        );
    }

    #[test]
    fn spirit_store_collects_only_exact_zero_candidates_before_removing_them() {
        let fixture = StoreFixture::new("collect-zero-candidates");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("first candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(9, 0, 0),
            ))
            .expect("first candidate accepted");
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("weak but real"),
                    certainty: Magnitude::Minimum,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(9, 1, 0),
            ))
            .expect("minimum record accepted");
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("high record"),
                    certainty: Magnitude::High,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(9, 2, 0),
            ))
            .expect("high record accepted");
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Correction,
                    description: Description::new("second candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(9, 3, 0),
            ))
            .expect("second candidate accepted");

        let collected = store
            .collect_removal_candidates(RemovalCandidateCollection::inline())
            .expect("candidates collected");
        let remaining = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::any(),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Any,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::SummaryOnly,
                },
            })
            .expect("remaining records observed");

        assert_eq!(
            collected.removed_identifiers(),
            &[RecordIdentifier::new(1), RecordIdentifier::new(4)]
        );
        assert_eq!(
            collected
                .archived_records()
                .iter()
                .map(|record| record.description.as_str())
                .collect::<Vec<_>>(),
            vec!["first candidate", "second candidate"]
        );
        let WorkingReply::RecordsObserved(records) = remaining else {
            panic!("expected summary records");
        };
        assert_eq!(
            records
                .records()
                .iter()
                .map(|record| record.identifier)
                .collect::<Vec<_>>(),
            vec![RecordIdentifier::new(2), RecordIdentifier::new(3)]
        );
    }

    #[test]
    fn spirit_store_rejects_non_zero_collection_query_without_retracting() {
        let fixture = StoreFixture::new("reject-broad-collection");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(10, 0, 0),
            ))
            .expect("candidate accepted");

        let error = store
            .collect_removal_candidates(RemovalCandidateCollection::new(
                RecordQuery {
                    topic_selection: TopicSelection::any(),
                    kind: None,
                    certainty_selection: CertaintySelection::Any,
                    recorded_time_selection: RecordedTimeSelection::Any,
                    privacy_selection: PrivacySelection::default_observation_privacy(),
                    mode: ObservationMode::SummaryOnly,
                },
                ArchiveTarget::Inline,
            ))
            .expect_err("broad query rejected");
        let candidates = store
            .observe_records(RecordObservation {
                query: RecordQuery::removal_candidates(ObservationMode::SummaryOnly),
            })
            .expect("candidate still observed");

        assert!(
            matches!(error, Error::RequestRejected { reason } if reason.contains("exact Zero"))
        );
        let WorkingReply::RecordsObserved(records) = candidates else {
            panic!("expected summary records");
        };
        assert_eq!(records.records().len(), 1);
        assert_eq!(records.records()[0].identifier, RecordIdentifier::new(1));
    }

    #[test]
    fn spirit_store_archive_file_failure_preserves_candidates() {
        let fixture = StoreFixture::new("archive-failure");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 3),
                Time::new(11, 0, 0),
            ))
            .expect("candidate accepted");
        let mut directory = std::env::temp_dir();
        directory.push(format!(
            "persona-spirit-archive-failure-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&directory).expect("archive failure directory created");

        let collected = store
            .collect_removal_candidates(RemovalCandidateCollection::new(
                RecordQuery::removal_candidates(ObservationMode::SummaryOnly),
                ArchiveTarget::File(ArchivePath::new(directory.to_string_lossy().into_owned())),
            ))
            .expect("directory target produces skipped archive receipt");
        let candidates = store
            .observe_records(RecordObservation {
                query: RecordQuery::removal_candidates(ObservationMode::SummaryOnly),
            })
            .expect("candidate still observed");

        assert!(collected.archived_records().is_empty());
        assert!(collected.removed_identifiers().is_empty());
        assert_eq!(collected.skipped_candidates().len(), 1);
        assert_eq!(
            collected.skipped_candidates()[0].reason,
            RemovalCandidateSkipReason::ArchiveFailed
        );
        let WorkingReply::RecordsObserved(records) = candidates else {
            panic!("expected summary records");
        };
        assert_eq!(records.records().len(), 1);
        assert_eq!(records.records()[0].identifier, RecordIdentifier::new(1));
    }
}
