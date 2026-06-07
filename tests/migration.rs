use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use nota_next::NotaSource;
use persona_spirit::{
    MigrationConfiguration, SpiritStore, StoreLocation, StorePath,
    migration::{IdentifierMigrationTable, ShortIdentifierMigrationTable},
    store::StampedEntry,
};
use sema::SchemaVersion;
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, RecordKey, TableDescriptor, TableName,
};
use signal_persona_spirit::{
    CertaintySelection, Date, Description, Entry, Kind, Magnitude, ObservationMode,
    PrivacySelection, RecordIdentifier, RecordObservation, RecordQuery, RecordedTimeSelection,
    Reply as WorkingReply, Time, Topic, TopicSelection, Topics,
    migration::{v010, v020, v030},
};

const V010_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const V020_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const V030_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const V040_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(4);
const V050_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(5);
const RECORDS: TableName = TableName::new("records");
const MINIMUM_IDENTIFIER_CODE_LENGTH: usize = 4;
const MAXIMUM_IDENTIFIER_CODE_LENGTH: usize = 7;

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
struct V040RecordIdentifier(u64);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V040StoredRecord {
    identifier: V040RecordIdentifier,
    entry: StampedEntry,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct V050StoredRecord {
    identifier: RecordIdentifier,
    entry: StampedEntry,
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

fn assert_short_identifier(identifier: RecordIdentifier) {
    let code_length = identifier.code().len();
    assert!(
        (MINIMUM_IDENTIFIER_CODE_LENGTH..=MAXIMUM_IDENTIFIER_CODE_LENGTH).contains(&code_length),
        "identifier code should be 4-7 characters: {}",
        identifier.code()
    );
}

impl MigrationFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut source = std::env::temp_dir();
        source.push(format!("persona-spirit-{test_name}-{nanos}-v010.sema"));
        let mut target = std::env::temp_dir();
        target.push(format!("persona-spirit-{test_name}-{nanos}-v020.sema"));
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
    assert_eq!(records[0].summary.identifier, RecordIdentifier::new(1));
    assert_eq!(
        records[0].summary.topics,
        Topics::single(Topic::new("spirit"))
    );
    assert_eq!(records[0].summary.kind, Kind::Decision);
    assert_eq!(
        records[0].summary.description,
        Description::new("first summary")
    );
    assert_eq!(records[0].summary.certainty, Magnitude::Maximum);
    assert_eq!(records[0].date, Date::new(2026, 5, 19));
    assert_eq!(records[0].time, Time::new(10, 15, 1));
    assert_eq!(records[1].summary.identifier, RecordIdentifier::new(2));
    assert_eq!(
        records[1].summary.topics,
        Topics::single(Topic::new("schema"))
    );
    assert_eq!(records[1].summary.kind, Kind::Principle);
    assert_eq!(
        records[1].summary.description,
        Description::new("second summary")
    );
    assert_eq!(records[1].summary.certainty, Magnitude::Medium);
    assert_eq!(records[1].date, Date::new(2026, 5, 20));
    assert_eq!(records[1].time, Time::new(11, 30, 2));

    let target =
        SpiritStore::open(&StoreLocation::new(fixture.target.as_path())).expect("target reopens");
    let accepted = target
        .assert_entry(StampedEntry::new(
            Entry {
                topics: Topics::single(Topic::new("next")),
                kind: Kind::Clarification,
                description: Description::new("post migration"),
                certainty: Magnitude::High,
                privacy: Magnitude::Zero,
            },
            Date::new(2026, 5, 21),
            Time::new(12, 45, 3),
        ))
        .expect("post-migration record accepted");
    assert_ne!(accepted.identifier(), RecordIdentifier::new(3));
    assert_short_identifier(accepted.identifier());
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
                topics: Topics::single(Topic::new("existing")),
                kind: Kind::Correction,
                description: Description::new("already here"),
                certainty: Magnitude::Maximum,
                privacy: Magnitude::Zero,
            },
            Date::new(2026, 5, 21),
            Time::new(12, 45, 3),
        ))
        .expect("target seeded");
    drop(target);

    let error = fixture.configuration().migrate().unwrap_err();

    assert!(
        error.to_string().contains("target database must be empty"),
        "unexpected error: {error}"
    );
}

#[test]
fn spirit_next_migration_projects_v020_topic_to_topic_vector() {
    let fixture = MigrationFixture::new("v020-to-next");
    write_v020_source(
        &fixture.source,
        vec![
            V020StoredRecord {
                identifier: RecordIdentifier::new(1),
                entry: V020StampedEntry {
                    entry: v020::Entry {
                        topic: v020::Topic::new("spirit"),
                        kind: v020::Kind::Correction,
                        description: v020::Description::new("single topic source"),
                        certainty: Magnitude::High,
                    },
                    date: Date::new(2026, 5, 25),
                    time: Time::new(21, 15, 0),
                },
            },
            V020StoredRecord {
                identifier: RecordIdentifier::new(2),
                entry: V020StampedEntry {
                    entry: v020::Entry {
                        topic: v020::Topic::new("nota"),
                        kind: v020::Kind::Principle,
                        description: v020::Description::new("second source"),
                        certainty: Magnitude::Maximum,
                    },
                    date: Date::new(2026, 5, 25),
                    time: Time::new(21, 16, 0),
                },
            },
        ],
    );

    let outcome = fixture
        .configuration()
        .migrate_v020_to_next()
        .expect("migration succeeds");

    assert_eq!(outcome.records(), 2);
    let records = target_provenances(&fixture.target);
    assert_eq!(
        records[0].summary.topics,
        Topics::single(Topic::new("spirit"))
    );
    assert_eq!(records[0].summary.certainty, Magnitude::High);
    assert_eq!(records[0].date, Date::new(2026, 5, 25));
    assert_eq!(records[0].time, Time::new(21, 15, 0));
    assert_eq!(
        records[1].summary.topics,
        Topics::single(Topic::new("nota"))
    );
}

#[test]
fn spirit_next_migration_binary_reads_one_nota_argument_and_writes_completed_reply() {
    let fixture = MigrationFixture::new("v020-next-binary");
    write_v020_source(
        &fixture.source,
        vec![V020StoredRecord {
            identifier: RecordIdentifier::new(1),
            entry: V020StampedEntry {
                entry: v020::Entry {
                    topic: v020::Topic::new("spirit"),
                    kind: v020::Kind::Decision,
                    description: v020::Description::new("binary next"),
                    certainty: Magnitude::Maximum,
                },
                date: Date::new(2026, 5, 25),
                time: Time::new(21, 20, 0),
            },
        }],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-2-to-next"))
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
        "(MigrationCompleted (1))"
    );
    let records = target_provenances(&fixture.target);
    assert_eq!(
        records[0].summary.topics,
        Topics::single(Topic::new("spirit"))
    );
}

#[test]
fn spirit_privacy_migration_projects_v030_records_to_v040() {
    let fixture = MigrationFixture::new("v030-v040");
    write_v030_source(
        &fixture.source,
        vec![
            V030StoredRecord {
                identifier: RecordIdentifier::new(1),
                entry: V030StampedEntry {
                    entry: v030::Entry {
                        topics: v030::Topics::new(vec![
                            v030::Topic::new("spirit"),
                            v030::Topic::new("privacy"),
                        ]),
                        kind: v030::Kind::Constraint,
                        description: v030::Description::new("privacy defaults open"),
                        certainty: Magnitude::Maximum,
                    },
                    date: Date::new(2026, 6, 4),
                    time: Time::new(12, 35, 0),
                },
            },
            V030StoredRecord {
                identifier: RecordIdentifier::new(2),
                entry: V030StampedEntry {
                    entry: v030::Entry {
                        topics: v030::Topics::single(v030::Topic::new("archive")),
                        kind: v030::Kind::Decision,
                        description: v030::Description::new("second survives"),
                        certainty: Magnitude::High,
                    },
                    date: Date::new(2026, 6, 4),
                    time: Time::new(12, 36, 0),
                },
            },
        ],
    );

    let outcome = fixture
        .configuration()
        .migrate_v030_to_v040()
        .expect("migration succeeds");

    assert_eq!(outcome.records(), 2);
    let records = target_provenances(&fixture.target);
    assert_eq!(records[0].summary.identifier, RecordIdentifier::new(1));
    assert_eq!(records[0].summary.topics.as_slice().len(), 2);
    assert_eq!(
        records[0].summary.topics.as_slice()[0],
        Topic::new("spirit")
    );
    assert_eq!(
        records[0].summary.topics.as_slice()[1],
        Topic::new("privacy")
    );
    assert_eq!(records[0].summary.kind, Kind::Constraint);
    assert_eq!(
        records[0].summary.description,
        Description::new("privacy defaults open")
    );
    assert_eq!(records[0].summary.certainty, Magnitude::Maximum);
    assert_eq!(records[0].summary.privacy, Magnitude::Zero);
    assert_eq!(records[1].summary.identifier, RecordIdentifier::new(2));
    assert_eq!(records[1].summary.privacy, Magnitude::Zero);

    let target =
        SpiritStore::open(&StoreLocation::new(fixture.target.as_path())).expect("target reopens");
    let accepted = target
        .assert_entry(StampedEntry::new(
            Entry {
                topics: Topics::single(Topic::new("post-migration")),
                kind: Kind::Clarification,
                description: Description::new("post privacy migration"),
                certainty: Magnitude::High,
                privacy: Magnitude::High,
            },
            Date::new(2026, 6, 4),
            Time::new(12, 37, 0),
        ))
        .expect("post-migration record accepted");
    assert_ne!(accepted.identifier(), RecordIdentifier::new(3));
    assert_short_identifier(accepted.identifier());
}

#[test]
fn spirit_identifier_migration_randomizes_ordinal_identifiers_and_writes_nota_mapping_table() {
    let fixture = MigrationFixture::new("v040-v050");
    write_v040_source(
        &fixture.source,
        vec![
            V040StoredRecord {
                identifier: V040RecordIdentifier(1),
                entry: StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("identity")),
                        kind: Kind::Decision,
                        description: Description::new("first ordinal"),
                        certainty: Magnitude::Maximum,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(13, 0, 0),
                ),
            },
            V040StoredRecord {
                identifier: V040RecordIdentifier(2),
                entry: StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("identity")),
                        kind: Kind::Correction,
                        description: Description::new("second ordinal"),
                        certainty: Magnitude::High,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(13, 1, 0),
                ),
            },
        ],
    );

    let outcome = fixture
        .configuration()
        .migrate_v040_to_v050()
        .expect("identifier migration succeeds");

    assert_eq!(outcome.records(), 2);
    let mapping = read_identifier_mapping_table(&fixture.target);
    assert_eq!(mapping.rows.len(), 2);
    assert_eq!(mapping.rows[0].ordinal_identifier, 1);
    assert_eq!(mapping.rows[1].ordinal_identifier, 2);
    assert_ne!(mapping.rows[0].hash_identifier, RecordIdentifier::new(1));
    assert_ne!(mapping.rows[1].hash_identifier, RecordIdentifier::new(2));
    assert_ne!(
        mapping.rows[0].hash_identifier,
        mapping.rows[1].hash_identifier
    );
    assert_short_identifier(mapping.rows[0].hash_identifier);
    assert_short_identifier(mapping.rows[1].hash_identifier);

    let records = target_provenances(&fixture.target);
    assert_eq!(records.len(), 2);
    let first = records
        .iter()
        .find(|record| record.summary.description == Description::new("first ordinal"))
        .expect("first record survives");
    let second = records
        .iter()
        .find(|record| record.summary.description == Description::new("second ordinal"))
        .expect("second record survives");
    assert_eq!(first.summary.identifier, mapping.rows[0].hash_identifier);
    assert_eq!(second.summary.identifier, mapping.rows[1].hash_identifier);
}

#[test]
fn spirit_identifier_migration_binary_reads_one_nota_argument_and_writes_completed_reply() {
    let fixture = MigrationFixture::new("v040-v050-binary");
    write_v040_source(
        &fixture.source,
        vec![V040StoredRecord {
            identifier: V040RecordIdentifier(7),
            entry: StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("identity")),
                    kind: Kind::Decision,
                    description: Description::new("binary identifier migration"),
                    certainty: Magnitude::Maximum,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(13, 5, 0),
            ),
        }],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-4-to-0-5"))
        .arg(fixture.configuration_text())
        .output()
        .expect("identifier migration binary runs");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "(MigrationCompleted (1))"
    );
    let mapping = read_identifier_mapping_table(&fixture.target);
    assert_eq!(mapping.rows[0].ordinal_identifier, 7);
}

#[test]
fn spirit_short_identifier_migration_preserves_short_ids_and_remints_long_ids() {
    let fixture = MigrationFixture::new("v050-v052");
    let existing_short_identifier = RecordIdentifier::from_code("abcd").expect("short id decodes");
    let existing_long_identifier =
        RecordIdentifier::from_code("20fzn9o0573n21mgujm").expect("long id decodes");
    write_v050_source(
        &fixture.source,
        vec![
            V050StoredRecord {
                identifier: existing_short_identifier,
                entry: StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("identity")),
                        kind: Kind::Decision,
                        description: Description::new("already short"),
                        certainty: Magnitude::High,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(17, 30, 0),
                ),
            },
            V050StoredRecord {
                identifier: existing_long_identifier,
                entry: StampedEntry::new(
                    Entry {
                        topics: Topics::single(Topic::new("identity")),
                        kind: Kind::Constraint,
                        description: Description::new("still long"),
                        certainty: Magnitude::High,
                        privacy: Magnitude::Zero,
                    },
                    Date::new(2026, 6, 4),
                    Time::new(17, 31, 0),
                ),
            },
        ],
    );

    let outcome = fixture
        .configuration()
        .migrate_v050_to_v052()
        .expect("short identifier migration succeeds");

    assert_eq!(outcome.records(), 2);
    let mapping = read_short_identifier_mapping_table(&fixture.target);
    assert_eq!(mapping.rows.len(), 2);
    let short_mapping = mapping
        .rows
        .iter()
        .find(|row| row.previous_identifier == existing_short_identifier)
        .expect("short mapping exists");
    assert_eq!(
        short_mapping.current_identifier, existing_short_identifier,
        "already-short identifiers should remain stable"
    );
    let long_mapping = mapping
        .rows
        .iter()
        .find(|row| row.previous_identifier == existing_long_identifier)
        .expect("long mapping exists");
    assert_ne!(long_mapping.current_identifier, existing_long_identifier);
    assert_short_identifier(long_mapping.current_identifier);

    let records = target_provenances(&fixture.target);
    assert_eq!(records.len(), 2);
    for record in records {
        assert_short_identifier(record.summary.identifier);
    }
}

#[test]
fn spirit_short_identifier_migration_binary_reads_one_nota_argument_and_writes_completed_reply() {
    let fixture = MigrationFixture::new("v050-v052-binary");
    write_v050_source(
        &fixture.source,
        vec![V050StoredRecord {
            identifier: RecordIdentifier::from_code("5y4b9i6swapgd4yswt4")
                .expect("long id decodes"),
            entry: StampedEntry::new(
                Entry {
                    topics: Topics::single(Topic::new("identity")),
                    kind: Kind::Decision,
                    description: Description::new("binary short identifier migration"),
                    certainty: Magnitude::High,
                    privacy: Magnitude::Zero,
                },
                Date::new(2026, 6, 4),
                Time::new(17, 35, 0),
            ),
        }],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-5-to-0-5-2"))
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
        "(MigrationCompleted (1))"
    );
    let mapping = read_short_identifier_mapping_table(&fixture.target);
    assert_eq!(mapping.rows.len(), 1);
    assert_short_identifier(mapping.rows[0].current_identifier);
}

#[test]
fn spirit_privacy_migration_binary_reads_one_nota_argument_and_writes_completed_reply() {
    let fixture = MigrationFixture::new("v030-v040-binary");
    write_v030_source(
        &fixture.source,
        vec![V030StoredRecord {
            identifier: RecordIdentifier::new(1),
            entry: V030StampedEntry {
                entry: v030::Entry {
                    topics: v030::Topics::single(v030::Topic::new("spirit")),
                    kind: v030::Kind::Decision,
                    description: v030::Description::new("binary privacy"),
                    certainty: Magnitude::Maximum,
                },
                date: Date::new(2026, 6, 4),
                time: Time::new(12, 40, 0),
            },
        }],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_spirit-migrate-0-3-to-0-4"))
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
        "(MigrationCompleted (1))"
    );
    let records = target_provenances(&fixture.target);
    assert_eq!(records[0].summary.privacy, Magnitude::Zero);
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
        "(MigrationCompleted (2))"
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
        "(MigrationCompleted (1))"
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

fn write_v020_source(path: &StorePath, records: Vec<V020StoredRecord>) {
    let mut engine = Engine::open(EngineOpen::new(path.as_path(), V020_SCHEMA_VERSION))
        .expect("v0.2 engine opens");
    let table = engine
        .register_table(TableDescriptor::new(RECORDS))
        .expect("v0.2 records table registers");
    for record in records {
        engine
            .assert(Assertion::new(table, record))
            .expect("v0.2 record writes");
    }
}

fn write_v030_source(path: &StorePath, records: Vec<V030StoredRecord>) {
    let mut engine = Engine::open(EngineOpen::new(path.as_path(), V030_SCHEMA_VERSION))
        .expect("v0.3 engine opens");
    let table = engine
        .register_table(TableDescriptor::new(RECORDS))
        .expect("v0.3 records table registers");
    for record in records {
        engine
            .assert(Assertion::new(table, record))
            .expect("v0.3 record writes");
    }
}

fn write_v040_source(path: &StorePath, records: Vec<V040StoredRecord>) {
    let mut engine = Engine::open(EngineOpen::new(path.as_path(), V040_SCHEMA_VERSION))
        .expect("v0.4 engine opens");
    let table = engine
        .register_table(TableDescriptor::new(RECORDS))
        .expect("v0.4 records table registers");
    for record in records {
        engine
            .assert(Assertion::new(table, record))
            .expect("v0.4 record writes");
    }
}

fn write_v050_source(path: &StorePath, records: Vec<V050StoredRecord>) {
    let mut engine = Engine::open(EngineOpen::new(path.as_path(), V050_SCHEMA_VERSION))
        .expect("v0.5 engine opens");
    let table = engine
        .register_table(TableDescriptor::new(RECORDS))
        .expect("v0.5 records table registers");
    for record in records {
        engine
            .assert(Assertion::new(table, record))
            .expect("v0.5 record writes");
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
                topic_selection: TopicSelection::any(),
                kind: None,
                certainty_selection: CertaintySelection::Any,
                recorded_time_selection: RecordedTimeSelection::Any,
                privacy_selection: PrivacySelection::default_observation_privacy(),
                mode: ObservationMode::WithProvenance,
            },
        })
        .expect("records observed");
    match reply {
        WorkingReply::RecordProvenancesObserved(records) => records.into_records(),
        other => panic!("expected provenance reply, got {other:?}"),
    }
}

fn read_identifier_mapping_table(target: &StorePath) -> IdentifierMigrationTable {
    let text = fs::read_to_string(identifier_mapping_table_path(target)).expect("mapping reads");
    NotaSource::new(&text)
        .parse::<IdentifierMigrationTable>()
        .expect("mapping decodes")
}

fn read_short_identifier_mapping_table(target: &StorePath) -> ShortIdentifierMigrationTable {
    let text =
        fs::read_to_string(short_identifier_mapping_table_path(target)).expect("mapping reads");
    NotaSource::new(&text)
        .parse::<ShortIdentifierMigrationTable>()
        .expect("mapping decodes")
}

fn identifier_mapping_table_path(target: &StorePath) -> std::path::PathBuf {
    let mut path = target.as_path().to_path_buf();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}.identifier-migration.nota"))
        .unwrap_or_else(|| "spirit.identifier-migration.nota".to_string());
    path.set_file_name(file_name);
    path
}

fn short_identifier_mapping_table_path(target: &StorePath) -> std::path::PathBuf {
    let mut path = target.as_path().to_path_buf();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}.short-identifier-migration.nota"))
        .unwrap_or_else(|| "spirit.short-identifier-migration.nota".to_string());
    path.set_file_name(file_name);
    path
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
        RecordKey::new(self.identifier.0.to_string())
    }
}

impl EngineRecord for V050StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.identifier.code())
    }
}
