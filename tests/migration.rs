use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{
    MigrationConfiguration, SpiritStore, StoreLocation, StorePath, store::StampedEntry,
};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, RecordKey, TableDescriptor, TableName,
};
use signal_persona_spirit::{
    Date, Description, Entry, Kind, ObservationMode, RecordIdentifier, RecordObservation,
    RecordQuery, Reply as WorkingReply, Time, Topic, migration::v010,
};
use signal_sema::Magnitude;

const V010_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const RECORDS: TableName = TableName::new("records");

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

#[derive(Debug, Clone)]
struct OldRecordInput<'a> {
    identifier: u64,
    topic: &'a str,
    kind: v010::Kind,
    summary: &'a str,
    context: &'a str,
    certainty: v010::Certainty,
    quote: &'a str,
    date: Date,
    time: Time,
}

#[derive(Debug, Clone)]
struct MigrationFixture {
    source: StorePath,
    target: StorePath,
}

impl MigrationFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut source = std::env::temp_dir();
        source.push(format!("persona-spirit-{test_name}-{nanos}-v010.redb"));
        let mut target = std::env::temp_dir();
        target.push(format!("persona-spirit-{test_name}-{nanos}-v020.redb"));
        Self {
            source: StorePath::new(source.to_string_lossy().into_owned()),
            target: StorePath::new(target.to_string_lossy().into_owned()),
        }
    }

    fn configuration(&self) -> MigrationConfiguration {
        MigrationConfiguration::new(self.source.clone(), self.target.clone())
    }

    fn configuration_text(&self) -> String {
        format!(
            "([{}] [{}])",
            self.source.as_path().display(),
            self.target.as_path().display()
        )
    }
}

#[test]
fn spirit_migration_preserves_timestamp_and_identifier_order() {
    let fixture = MigrationFixture::new("preserves-time");
    write_v010_source(
        &fixture.source,
        vec![
            old_record(OldRecordInput {
                identifier: 2,
                topic: "schema",
                kind: v010::Kind::Principle,
                summary: "second summary",
                context: "second context should disappear",
                certainty: v010::Certainty::Medium,
                quote: "second quote should disappear",
                date: Date::new(2026, 5, 20),
                time: Time::new(11, 30, 2),
            }),
            old_record(OldRecordInput {
                identifier: 1,
                topic: "spirit",
                kind: v010::Kind::Decision,
                summary: "first summary",
                context: "first context should disappear",
                certainty: v010::Certainty::Maximum,
                quote: "first quote should disappear",
                date: Date::new(2026, 5, 19),
                time: Time::new(10, 15, 1),
            }),
        ],
    );

    let outcome = fixture
        .configuration()
        .migrate()
        .expect("migration succeeds");

    assert_eq!(outcome.records(), 2);
    let records = target_provenances(&fixture.target);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].description.identifier, RecordIdentifier::new(1));
    assert_eq!(records[0].description.topic, Topic::new("spirit"));
    assert_eq!(records[0].description.kind, Kind::Decision);
    assert_eq!(
        records[0].description.description,
        Description::new("first summary")
    );
    assert_eq!(records[0].description.certainty, Magnitude::Maximum);
    assert_eq!(records[0].date, Date::new(2026, 5, 19));
    assert_eq!(records[0].time, Time::new(10, 15, 1));
    assert_eq!(records[1].description.identifier, RecordIdentifier::new(2));
    assert_eq!(records[1].description.topic, Topic::new("schema"));
    assert_eq!(records[1].description.kind, Kind::Principle);
    assert_eq!(
        records[1].description.description,
        Description::new("second summary")
    );
    assert_eq!(records[1].description.certainty, Magnitude::Medium);
    assert_eq!(records[1].date, Date::new(2026, 5, 20));
    assert_eq!(records[1].time, Time::new(11, 30, 2));

    let target =
        SpiritStore::open(&StoreLocation::new(fixture.target.as_path())).expect("target reopens");
    let accepted = target
        .assert_entry(StampedEntry::new(
            Entry {
                topic: Topic::new("next"),
                kind: Kind::Clarification,
                description: Description::new("post migration"),
                certainty: Magnitude::High,
            },
            Date::new(2026, 5, 21),
            Time::new(12, 45, 3),
        ))
        .expect("post-migration record accepted");
    assert_eq!(accepted.identifier(), RecordIdentifier::new(3));
}

#[test]
fn spirit_migration_refuses_non_empty_target() {
    let fixture = MigrationFixture::new("non-empty-target");
    write_v010_source(
        &fixture.source,
        vec![old_record(OldRecordInput {
            identifier: 1,
            topic: "spirit",
            kind: v010::Kind::Decision,
            summary: "source",
            context: "context",
            certainty: v010::Certainty::Maximum,
            quote: "quote",
            date: Date::new(2026, 5, 19),
            time: Time::new(10, 15, 1),
        })],
    );
    let target =
        SpiritStore::open(&StoreLocation::new(fixture.target.as_path())).expect("target opens");
    target
        .assert_entry(StampedEntry::new(
            Entry {
                topic: Topic::new("existing"),
                kind: Kind::Correction,
                description: Description::new("already here"),
                certainty: Magnitude::Maximum,
            },
            Date::new(2026, 5, 21),
            Time::new(12, 45, 3),
        ))
        .expect("target seeded");
    drop(target);

    let error = fixture.configuration().migrate().unwrap_err();

    assert!(
        error
            .to_string()
            .contains("target v0.2 database must be empty"),
        "unexpected error: {error}"
    );
}

#[test]
fn spirit_migration_binary_reads_one_nota_argument_and_writes_completed_reply() {
    let fixture = MigrationFixture::new("binary");
    write_v010_source(
        &fixture.source,
        vec![
            old_record(OldRecordInput {
                identifier: 1,
                topic: "spirit",
                kind: v010::Kind::Decision,
                summary: "binary first",
                context: "context",
                certainty: v010::Certainty::Maximum,
                quote: "quote",
                date: Date::new(2026, 5, 19),
                time: Time::new(10, 15, 1),
            }),
            old_record(OldRecordInput {
                identifier: 2,
                topic: "schema",
                kind: v010::Kind::Constraint,
                summary: "binary second",
                context: "context",
                certainty: v010::Certainty::Minimum,
                quote: "quote",
                date: Date::new(2026, 5, 20),
                time: Time::new(11, 30, 2),
            }),
        ],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-1-to-0-2"))
        .arg(fixture.configuration_text())
        .output()
        .expect("migration binary runs");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "(MigrationCompleted 2)"
    );
    let records = target_provenances(&fixture.target);
    assert_eq!(records[0].date, Date::new(2026, 5, 19));
    assert_eq!(records[1].date, Date::new(2026, 5, 20));
}

#[test]
fn spirit_migration_binary_accepts_configuration_file_path_argument() {
    let fixture = MigrationFixture::new("file-argument");
    write_v010_source(
        &fixture.source,
        vec![old_record(OldRecordInput {
            identifier: 1,
            topic: "spirit",
            kind: v010::Kind::Decision,
            summary: "file argument",
            context: "context",
            certainty: v010::Certainty::Maximum,
            quote: "quote",
            date: Date::new(2026, 5, 19),
            time: Time::new(10, 15, 1),
        })],
    );
    let mut configuration_path = std::env::temp_dir();
    configuration_path.push(format!(
        "persona-spirit-migration-{}.nota",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    fs::write(&configuration_path, fixture.configuration_text()).expect("configuration writes");

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-1-to-0-2"))
        .arg(&configuration_path)
        .output()
        .expect("migration binary runs");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "(MigrationCompleted 1)"
    );
}

fn write_v010_source(path: &StorePath, records: Vec<V010StoredRecord>) {
    let mut engine = Engine::open(EngineOpen::new(path.as_path(), V010_SCHEMA_VERSION))
        .expect("v0.1 engine opens");
    let table = engine
        .register_table(TableDescriptor::new(RECORDS))
        .expect("v0.1 records table registers");
    for record in records {
        engine
            .assert(Assertion::new(table, record))
            .expect("v0.1 record writes");
    }
}

fn old_record(input: OldRecordInput<'_>) -> V010StoredRecord {
    V010StoredRecord {
        identifier: RecordIdentifier::new(input.identifier),
        entry: V010StampedEntry {
            entry: v010::Entry {
                topic: v010::Topic::new(input.topic),
                kind: input.kind,
                summary: v010::Summary::new(input.summary),
                context: v010::Context::new(input.context),
                certainty: input.certainty,
                quote: v010::Quote::new(input.quote),
            },
            date: input.date,
            time: input.time,
        },
    }
}

fn target_provenances(target: &StorePath) -> Vec<signal_persona_spirit::RecordProvenance> {
    let store = SpiritStore::open(&StoreLocation::new(target.as_path())).expect("target opens");
    let reply = store
        .observe_records(RecordObservation {
            query: RecordQuery {
                topic: None,
                kind: None,
                mode: ObservationMode::WithProvenance,
            },
        })
        .expect("records observed");
    match reply {
        WorkingReply::RecordProvenancesObserved(records) => records.records,
        other => panic!("expected provenance reply, got {other:?}"),
    }
}

impl EngineRecord for V010StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.value().to_string())
    }
}
