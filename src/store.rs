use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sema::SchemaVersion;
use sema_engine::{
    Assertion, CommitLogEntry, Engine, EngineOpen, EngineRecord, Mutation, QueryPlan, RecordKey,
    Retraction, TableDescriptor, TableName, TableReference,
};
use signal_persona_spirit::{
    ArchiveDatabaseTarget, Certainty, CertaintyChange, CertaintyChanged, CertaintySelection, Date,
    Entry, Kind, ObservationMode, OutputTarget, PrivacySelection, RecordAccepted, RecordIdentifier,
    RecordObservation, RecordProvenance, RecordProvenancesObserved, RecordQuery, RecordRemoved,
    RecordSummary, RecordedTime, RecordedTimeSelection, RecordsObserved,
    RemovalCandidateCollection, RemovalCandidateSkipReason, RemovalCandidatesCollected,
    Reply as WorkingReply, SkippedRemovalCandidate, Time, Topic, TopicCount, TopicSelection,
    Topics, TopicsObserved,
};
use signal_version_handover::{HandoverMarker, MarkerRequest};
use version_projection::{ComponentName, ContractVersion, Projected};

use crate::{
    Result,
    error::{Error, RequestRejectionReason},
    observation::RecordIdentifierObservation,
};

const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(4);
const REMOVAL_CANDIDATE_ARCHIVE_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const SPIRIT_CONTRACT_VERSION: ContractVersion = ContractVersion::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0,
]);
const RECORDS: TableName = TableName::new("records");
const REMOVAL_CANDIDATE_ARCHIVE_RECORDS: TableName = TableName::new("removal_candidate_records");
const DEFAULT_STORE_PATH: &str = "/tmp/persona-spirit.redb";
const DEFAULT_REMOVAL_CANDIDATE_ARCHIVE_FILE_NAME: &str = "removal-candidate-archive.sema";
const BACKUP_REMOVAL_CANDIDATE_ARCHIVE_FILE_NAME: &str =
    "persona-spirit-removal-candidate-archive-backup.sema";
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
    location: StoreLocation,
    engine: Engine,
    records: TableReference<StoredRecord>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRecord {
    identifier: RecordIdentifier,
    entry: StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct ArchivedRemovalCandidate {
    summary: RecordSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordIdentifierMint {
    next: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordIdentifierCommitLog<'entry> {
    entries: &'entry [CommitLogEntry],
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

    fn default_removal_candidate_archive_path(&self) -> PathBuf {
        let archive_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("{name}.{DEFAULT_REMOVAL_CANDIDATE_ARCHIVE_FILE_NAME}"))
            .unwrap_or_else(|| DEFAULT_REMOVAL_CANDIDATE_ARCHIVE_FILE_NAME.to_string());
        self.path
            .parent()
            .map(|parent| parent.join(&archive_name))
            .unwrap_or_else(|| std::env::temp_dir().join(archive_name))
    }

    fn backup_removal_candidate_archive_path(&self) -> PathBuf {
        std::env::temp_dir().join(BACKUP_REMOVAL_CANDIDATE_ARCHIVE_FILE_NAME)
    }
}

impl SpiritStore {
    pub fn open(location: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(location.as_path(), SPIRIT_SCHEMA_VERSION))
            .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self {
            location: location.clone(),
            engine,
            records,
        })
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
        if archive
            .capture_to_target(&collection.output_target, &self.location)
            .is_err()
        {
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

    pub fn observe_record_identifiers(
        &self,
        observation: RecordIdentifierObservation,
    ) -> Result<WorkingReply> {
        let mode = observation.query.mode;
        let records = self.records_for_identifier_query(observation)?;
        Ok(RecordReply::new(records, mode).into_working_reply())
    }

    fn records_for_identifier_query(
        &self,
        observation: RecordIdentifierObservation,
    ) -> Result<Vec<StoredRecord>> {
        Ok(self
            .all_records()?
            .into_iter()
            .filter(|record| observation.query.contains(record.identifier))
            .filter(|record| {
                observation
                    .privacy_selection
                    .matches(record.entry.entry.privacy)
            })
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
            self.topic_counts(PrivacySelection::default_observation_privacy())?,
        )))
    }

    pub fn summaries_for_topic(
        &self,
        topic: Option<&Topic>,
        privacy_selection: PrivacySelection,
    ) -> Result<Vec<RecordSummary>> {
        let topic_selection = topic
            .cloned()
            .map(|topic| TopicSelection::partial(vec![topic]))
            .unwrap_or_else(TopicSelection::any);
        let query = RecordQuery {
            topic_selection,
            kind: None,
            certainty_selection: CertaintySelection::Any,
            recorded_time_selection: RecordedTimeSelection::Any,
            privacy_selection,
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
        let commit_log = self.engine.commit_log().map_err(Error::spirit_store)?;
        let commit_sequence = self
            .engine
            .current_commit_sequence()
            .map_err(Error::spirit_store)?
            .value();
        Ok(
            RecordIdentifierMint::from_records_commit_log_and_commit_sequence(
                &records,
                &commit_log,
                commit_sequence,
            )
            .next_identifier(),
        )
    }

    fn last_record_identifier(&self) -> Result<Option<u64>> {
        let live_identifier = self
            .all_records()?
            .last()
            .map(|record| record.identifier.value());
        let logged_identifier =
            RecordIdentifierCommitLog::new(&self.engine.commit_log().map_err(Error::spirit_store)?)
                .last_record_identifier();
        Ok(live_identifier.into_iter().chain(logged_identifier).max())
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
                reason: RequestRejectionReason::RecordNotStored {
                    collection: RECORDS.as_str().to_string(),
                    identifier: identifier.value(),
                },
            })
    }

    fn topic_counts(&self, privacy_selection: PrivacySelection) -> Result<Vec<TopicCount>> {
        let mut counts = BTreeMap::<String, u64>::new();
        for record in self.all_records()? {
            if !privacy_selection.matches(record.entry.entry.privacy) {
                continue;
            }
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
                reason: RequestRejectionReason::EmptyTopics,
            });
        }
        let mut seen = std::collections::BTreeSet::<&str>::new();
        for topic in topics.as_slice() {
            if !seen.insert(topic.as_str()) {
                return Err(Error::RequestRejected {
                    reason: RequestRejectionReason::DuplicateTopic {
                        topic: topic.as_str().to_string(),
                    },
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
            reason: RequestRejectionReason::CollectionQueryNotExactZeroPublic,
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

    fn capture_to_target(
        &self,
        target: &OutputTarget,
        store_location: &StoreLocation,
    ) -> Result<()> {
        match target {
            OutputTarget::ArchiveDatabase(ArchiveDatabaseTarget::Default) => {
                self.write_to_default_archive(store_location)
            }
            OutputTarget::ArchiveDatabase(ArchiveDatabaseTarget::Path(path)) => {
                self.write_to_archive_database(Path::new(path.as_str()))
            }
            OutputTarget::Print(_) => Ok(()),
        }
    }

    fn write_to_default_archive(&self, store_location: &StoreLocation) -> Result<()> {
        self.write_to_archive_database(&store_location.default_removal_candidate_archive_path())
            .or_else(|_| {
                self.write_to_archive_database(
                    &store_location.backup_removal_candidate_archive_path(),
                )
            })
    }

    fn write_to_archive_database(&self, path: &Path) -> Result<()> {
        let archive = RemovalCandidateArchiveStore::open(path)?;
        archive.append(self.records.clone())
    }
}

struct RemovalCandidateArchiveStore {
    engine: Engine,
    records: TableReference<ArchivedRemovalCandidate>,
}

impl RemovalCandidateArchiveStore {
    fn open(path: &Path) -> Result<Self> {
        let mut engine = Engine::open(EngineOpen::new(
            path,
            REMOVAL_CANDIDATE_ARCHIVE_SCHEMA_VERSION,
        ))
        .map_err(Error::spirit_store)?;
        let records = engine
            .register_table(TableDescriptor::new(REMOVAL_CANDIDATE_ARCHIVE_RECORDS))
            .map_err(Error::spirit_store)?;
        Ok(Self { engine, records })
    }

    fn append(&self, records: Vec<RecordSummary>) -> Result<()> {
        for summary in records {
            self.engine
                .assert(Assertion::new(
                    self.records,
                    ArchivedRemovalCandidate { summary },
                ))
                .map_err(Error::spirit_store)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn summaries(&self) -> Result<Vec<RecordSummary>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.records))
            .map_err(Error::spirit_store)?
            .records()
            .iter()
            .map(|record| record.summary.clone())
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

impl EngineRecord for ArchivedRemovalCandidate {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.summary.identifier.value().to_string())
    }
}

impl RecordIdentifierMint {
    fn from_records_commit_log_and_commit_sequence(
        records: &[StoredRecord],
        commit_log: &[CommitLogEntry],
        commit_sequence: u64,
    ) -> Self {
        let last_record_identifier = records
            .iter()
            .map(|record| record.identifier.value())
            .max()
            .unwrap_or(0);
        let last_logged_identifier = RecordIdentifierCommitLog::new(commit_log)
            .last_record_identifier()
            .unwrap_or(0);
        let next = last_record_identifier
            .max(last_logged_identifier)
            .max(commit_sequence)
            + 1;
        Self { next }
    }

    fn next_identifier(&self) -> RecordIdentifier {
        RecordIdentifier::new(self.next)
    }
}

impl<'entry> RecordIdentifierCommitLog<'entry> {
    fn new(entries: &'entry [CommitLogEntry]) -> Self {
        Self { entries }
    }

    fn last_record_identifier(&self) -> Option<u64> {
        self.entries
            .iter()
            .flat_map(|entry| entry.operations().iter())
            .filter(|operation| operation.table_name() == RECORDS.as_str())
            .filter_map(|operation| operation.key())
            .filter_map(|key| key.as_str().parse::<u64>().ok())
            .max()
    }
}

#[cfg(test)]
mod tests {
    use signal_persona_spirit::{
        Description, Kind, OutputTarget, RecordedTimeRange, RemovalCandidateCollection,
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
    fn spirit_store_does_not_reset_identifier_high_water_after_imported_record_removal() {
        let fixture = StoreFixture::new("imported-high-water-removal");
        let store = fixture.store();
        let imported_identifier = RecordIdentifier::new(1_000);
        store
            .import_migrated_record(
                imported_identifier,
                StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("spirit")),
                        kind: Kind::Decision,
                        description: Description::new("imported high identifier"),
                        certainty: Magnitude::Maximum,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(9, 0, 0),
                ),
            )
            .expect("high identifier imported");
        store
            .remove_entry(imported_identifier)
            .expect("high identifier removed");

        let accepted = store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Principle,
                    description: Description::new("after high removal"),
                    certainty: Magnitude::High,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(9, 1, 0),
            ))
            .expect("post-removal record accepted");

        assert_eq!(accepted.identifier(), RecordIdentifier::new(1_001));
    }

    #[test]
    fn spirit_store_handover_marker_keeps_removed_identifier_high_water() {
        let fixture = StoreFixture::new("handover-marker-high-water");
        let store = fixture.store();
        let imported_identifier = RecordIdentifier::new(1_000);
        store
            .import_migrated_record(
                imported_identifier,
                StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("spirit")),
                        kind: Kind::Decision,
                        description: Description::new("imported high identifier"),
                        certainty: Magnitude::Maximum,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(9, 0, 0),
                ),
            )
            .expect("high identifier imported");
        store
            .remove_entry(imported_identifier)
            .expect("high identifier removed");

        let marker = store
            .handover_marker(
                MarkerRequest {
                    component: ComponentName::new("persona-spirit"),
                },
                spirit_contract_version(),
            )
            .expect("handover marker read");

        assert_eq!(marker.last_record_identifier, Some(1_000));
    }

    #[test]
    fn spirit_store_does_not_reuse_minted_identifier_after_sparse_migration() {
        let fixture = StoreFixture::new("sparse-migration-minted-removal");
        let store = fixture.store();
        store
            .import_migrated_record(
                RecordIdentifier::new(1_000),
                StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("spirit")),
                        kind: Kind::Decision,
                        description: Description::new("imported high identifier"),
                        certainty: Magnitude::Maximum,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(9, 0, 0),
                ),
            )
            .expect("high identifier imported");
        let first_after_import = store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Principle,
                    description: Description::new("first after import"),
                    certainty: Magnitude::High,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(9, 1, 0),
            ))
            .expect("first post-import record accepted");
        store
            .remove_entry(first_after_import.identifier())
            .expect("minted high identifier removed");

        let second_after_import = store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Correction,
                    description: Description::new("second after import"),
                    certainty: Magnitude::Medium,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(9, 2, 0),
            ))
            .expect("second post-import record accepted");

        assert_eq!(
            first_after_import.identifier(),
            RecordIdentifier::new(1_001)
        );
        assert_eq!(
            second_after_import.identifier(),
            RecordIdentifier::new(1_002)
        );
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
            .collect_removal_candidates(RemovalCandidateCollection::default_archive_database())
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
        let archive = RemovalCandidateArchiveStore::open(
            &fixture.location.default_removal_candidate_archive_path(),
        )
        .expect("default archive opens");
        assert_eq!(
            archive
                .summaries()
                .expect("archived summaries read")
                .iter()
                .map(|record| record.description.as_str())
                .collect::<Vec<_>>(),
            vec!["first candidate", "second candidate"]
        );
    }

    #[test]
    fn spirit_store_print_target_collects_without_archive_database() {
        let fixture = StoreFixture::new("print-zero-candidates");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Correction,
                    description: Description::new("printed candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(12, 0, 0),
            ))
            .expect("candidate accepted");

        let collected = store
            .collect_removal_candidates(RemovalCandidateCollection::print_standard_output())
            .expect("print target collects candidates");
        let remaining = store
            .observe_records(RecordObservation {
                query: RecordQuery::removal_candidates(ObservationMode::SummaryOnly),
            })
            .expect("remaining candidates observed");

        assert_eq!(
            collected
                .archived_records()
                .iter()
                .map(|record| record.description.as_str())
                .collect::<Vec<_>>(),
            vec!["printed candidate"]
        );
        assert_eq!(collected.removed_identifiers(), &[RecordIdentifier::new(1)]);
        assert!(
            !fixture
                .location
                .default_removal_candidate_archive_path()
                .exists()
        );
        let WorkingReply::RecordsObserved(records) = remaining else {
            panic!("expected summary records");
        };
        assert!(records.records().is_empty());
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
                OutputTarget::default_archive_database(),
            ))
            .expect_err("broad query rejected");
        let candidates = store
            .observe_records(RecordObservation {
                query: RecordQuery::removal_candidates(ObservationMode::SummaryOnly),
            })
            .expect("candidate still observed");

        assert!(matches!(
            error,
            Error::RequestRejected {
                reason: RequestRejectionReason::CollectionQueryNotExactZeroPublic
            }
        ));
        let WorkingReply::RecordsObserved(records) = candidates else {
            panic!("expected summary records");
        };
        assert_eq!(records.records().len(), 1);
        assert_eq!(records.records()[0].identifier, RecordIdentifier::new(1));
    }

    #[test]
    fn spirit_store_rejects_private_collection_query_without_retracting() {
        let fixture = StoreFixture::new("reject-private-collection");
        let store = fixture.store();
        store
            .assert_entry(StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("spirit")),
                    kind: Kind::Decision,
                    description: Description::new("private candidate"),
                    certainty: Magnitude::Zero,
                    privacy: Magnitude::High,
                },
                Date::new(2026, 6, 4),
                Time::new(10, 30, 0),
            ))
            .expect("candidate accepted");

        let error = store
            .collect_removal_candidates(RemovalCandidateCollection::new(
                RecordQuery {
                    topic_selection: TopicSelection::any(),
                    kind: None,
                    certainty_selection: CertaintySelection::removal_candidates(),
                    recorded_time_selection: RecordedTimeSelection::Any,
                    privacy_selection: PrivacySelection::AtMost(Magnitude::High),
                    mode: ObservationMode::SummaryOnly,
                },
                OutputTarget::default_archive_database(),
            ))
            .expect_err("private collection query rejected");
        let candidates = store
            .observe_records(RecordObservation {
                query: RecordQuery {
                    topic_selection: TopicSelection::any(),
                    kind: None,
                    certainty_selection: CertaintySelection::removal_candidates(),
                    recorded_time_selection: RecordedTimeSelection::Any,
                    privacy_selection: PrivacySelection::AtMost(Magnitude::High),
                    mode: ObservationMode::SummaryOnly,
                },
            })
            .expect("candidate still observed");

        assert!(matches!(
            error,
            Error::RequestRejected {
                reason: RequestRejectionReason::CollectionQueryNotExactZeroPublic
            }
        ));
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
                OutputTarget::archive_database(directory.to_string_lossy().into_owned()),
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
