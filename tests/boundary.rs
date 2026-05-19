use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{Error, SingleArgument, SpiritClient, StoreLocation};

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
        path.push(format!("persona-spirit-{test_name}-{nanos}.redb"));
        Self {
            location: StoreLocation::new(path),
        }
    }

    fn client(&self, text: &str) -> SpiritClient {
        let argument =
            SingleArgument::from_arguments(["persona-spirit".to_string(), text.to_string()])
                .expect("single argument accepted");
        SpiritClient::with_store(argument, self.location.clone())
    }
}

#[test]
fn persona_spirit_binary_accepts_exactly_one_argument() {
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(Statement \"capture this intent\")".to_string(),
    ])
    .expect("single argument accepted");

    assert_eq!(argument.as_str(), "(Statement \"capture this intent\")");
}

#[test]
fn persona_spirit_binary_rejects_missing_argument() {
    let error = SingleArgument::from_arguments(["persona-spirit".to_string()]).unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "persona-spirit".to_string(),
            found: 0,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_extra_argument() {
    let error = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(Statement \"one\")".to_string(),
        "(Statement \"two\")".to_string(),
    ])
    .unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "persona-spirit".to_string(),
            found: 2,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_flag_style_argument() {
    let error =
        SingleArgument::from_arguments(["persona-spirit".to_string(), "--help".to_string()])
            .unwrap_err();

    assert_eq!(
        error,
        Error::FlagArgument {
            program: "persona-spirit".to_string(),
            argument: "--help".to_string(),
        }
    );
}

#[test]
fn persona_spirit_client_type_checks_statement_without_inventing_storage_behavior() {
    let fixture = StoreFixture::new("statement");
    let reply = fixture
        .client("(Statement (\"capture this intent\"))")
        .reply_text()
        .expect("request type checked");

    assert_eq!(reply, "(RequestUnimplemented (Statement NotBuiltYet))");
}

#[test]
fn persona_spirit_client_asserts_entry_and_mints_record_identifier() {
    let fixture = StoreFixture::new("assert-entry");
    let reply = fixture
        .client("(Entry (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .reply_text()
        .expect("entry persisted");

    assert_eq!(
        reply,
        "(RecordAccepted ((1 workspace Decision \"summary only\" Maximum)))"
    );
}

#[test]
fn persona_spirit_client_persists_entries_for_later_summary_observation() {
    let fixture = StoreFixture::new("summary-observation");
    fixture
        .client("(Entry (workspace Decision \"first summary\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .reply_text()
        .expect("first entry persisted");
    fixture
        .client("(Entry (workspace Correction \"second summary\" \"current implementation context\" Medium \"2026-05-19T13:12:00Z\" \"second statement\"))")
        .reply_text()
        .expect("second entry persisted");

    let reply = fixture
        .client("(RecordObservation ((None SummaryOnly)))")
        .reply_text()
        .expect("records observed");

    assert_eq!(
        reply,
        "(RecordsObserved ([(1 workspace Decision \"first summary\" Maximum) (2 workspace Correction \"second summary\" Medium)]))"
    );
}

#[test]
fn persona_spirit_client_observes_default_psyche_state() {
    let fixture = StoreFixture::new("state-observation");
    let reply = fixture
        .client("(StateObservation ())")
        .reply_text()
        .expect("state observed");

    assert_eq!(reply, "(StateObserved ((Absent None)))");
}

#[test]
fn persona_spirit_client_observes_empty_pending_questions() {
    let fixture = StoreFixture::new("question-observation");
    let reply = fixture
        .client("(QuestionPending ())")
        .reply_text()
        .expect("questions observed");

    assert_eq!(reply, "(QuestionsObserved ([]))");
}

#[test]
fn persona_spirit_client_filters_record_observation_by_topic() {
    let fixture = StoreFixture::new("topic-filter");
    fixture
        .client("(Entry (workspace Decision \"workspace summary\" \"workspace context\" Maximum \"2026-05-19T13:08:11Z\" \"workspace quote\"))")
        .reply_text()
        .expect("workspace entry persisted");
    fixture
        .client("(Entry (naming Correction \"naming summary\" \"naming context\" Maximum \"2026-05-19T13:12:00Z\" \"naming quote\"))")
        .reply_text()
        .expect("naming entry persisted");

    let reply = fixture
        .client("(RecordObservation (((Some naming) SummaryOnly)))")
        .reply_text()
        .expect("records observed");

    assert_eq!(
        reply,
        "(RecordsObserved ([(2 naming Correction \"naming summary\" Maximum)]))"
    );
}

#[test]
fn persona_spirit_client_returns_provenance_only_when_requested() {
    let fixture = StoreFixture::new("provenance");
    fixture
        .client("(Entry (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .reply_text()
        .expect("entry persisted");

    let reply = fixture
        .client("(RecordObservation ((None WithProvenance)))")
        .reply_text()
        .expect("provenance observed");

    assert_eq!(
        reply,
        "(RecordProvenancesObserved ([((1 workspace Decision \"summary only\" Maximum) \"current implementation context\" \"2026-05-19T13:08:11Z\" \"first statement\")]))"
    );
}

#[test]
fn persona_spirit_client_repeated_entries_remain_distinct_records() {
    let fixture = StoreFixture::new("repetition");
    fixture
        .client("(Entry (naming Correction \"drop ancestor prefixes\" \"first context\" Maximum \"2026-05-19T13:08:11Z\" \"first wording\"))")
        .reply_text()
        .expect("first entry persisted");
    fixture
        .client("(Entry (naming Correction \"drop ancestor prefixes\" \"second context\" Maximum \"2026-05-19T13:12:00Z\" \"second wording\"))")
        .reply_text()
        .expect("second entry persisted");

    let reply = fixture
        .client("(RecordObservation (((Some naming) SummaryOnly)))")
        .reply_text()
        .expect("records observed");

    assert_eq!(
        reply,
        "(RecordsObserved ([(1 naming Correction \"drop ancestor prefixes\" Maximum) (2 naming Correction \"drop ancestor prefixes\" Maximum)]))"
    );
}

#[test]
fn persona_spirit_client_rejects_unknown_record_shape() {
    let fixture = StoreFixture::new("unknown-record");
    let error = fixture
        .client("(UnknownIntent workspace)")
        .reply_text()
        .unwrap_err();

    assert!(matches!(error, Error::InvalidSpiritRequest { .. }));
}
