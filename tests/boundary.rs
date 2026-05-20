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
        "(State \"capture this intent\")".to_string(),
    ])
    .expect("single argument accepted");

    assert_eq!(argument.as_str(), "(State \"capture this intent\")");
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
        "(State \"one\")".to_string(),
        "(State \"two\")".to_string(),
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
fn persona_spirit_client_classifies_statement_as_provisional_record() {
    let fixture = StoreFixture::new("statement");
    let reply = fixture
        .client("(State (\"capture this intent\"))")
        .reply_text()
        .expect("statement classified");

    assert_eq!(
        reply,
        "(RecordAccepted ((1 unclassified Clarification \"capture this intent\" Minimum)))"
    );
}

#[test]
fn persona_spirit_client_asserts_entry_and_mints_record_identifier() {
    let fixture = StoreFixture::new("assert-entry");
    let reply = fixture
        .client("(Record (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
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
        .client("(Record (workspace Decision \"first summary\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .reply_text()
        .expect("first entry persisted");
    fixture
        .client("(Record (workspace Correction \"second summary\" \"current implementation context\" Medium \"2026-05-19T13:12:00Z\" \"second statement\"))")
        .reply_text()
        .expect("second entry persisted");

    let reply = fixture
        .client("(Observe (Records (None SummaryOnly)))")
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
        .client("(Observe (State ()))")
        .reply_text()
        .expect("state observed");

    assert_eq!(reply, "(StateObserved ((Absent None)))");
}

#[test]
fn persona_spirit_client_observes_empty_pending_questions() {
    let fixture = StoreFixture::new("question-observation");
    let reply = fixture
        .client("(Observe (Questions ()))")
        .reply_text()
        .expect("questions observed");

    assert_eq!(reply, "(QuestionsObserved ([]))");
}

#[test]
fn persona_spirit_client_opens_and_retracts_state_subscription() {
    let fixture = StoreFixture::new("state-subscription");
    let opened = fixture
        .client("(Watch (State ()))")
        .reply_text()
        .expect("state subscription opened");
    let retracted = fixture
        .client("(Unwatch (State (1)))")
        .reply_text()
        .expect("state subscription retracted");

    assert_eq!(opened, "(StateSubscriptionOpened ((1) (Absent None)))");
    assert_eq!(retracted, "(StateSubscriptionRetracted ((1)))");
}

#[test]
fn persona_spirit_client_opens_record_subscription_with_summary_snapshot() {
    let fixture = StoreFixture::new("record-subscription");
    fixture
        .client("(Record (workspace Decision \"subscription summary\" \"workspace context\" Maximum \"2026-05-19T13:08:11Z\" \"workspace quote\"))")
        .reply_text()
        .expect("entry persisted");

    let opened = fixture
        .client("(Watch (Records (None SummaryOnly)))")
        .reply_text()
        .expect("record subscription opened");
    let retracted = fixture
        .client("(Unwatch (Records (1)))")
        .reply_text()
        .expect("record subscription retracted");

    assert_eq!(
        opened,
        "(RecordSubscriptionOpened ((1) [(1 workspace Decision \"subscription summary\" Maximum)]))"
    );
    assert_eq!(retracted, "(RecordSubscriptionRetracted ((1)))");
}

#[test]
fn persona_spirit_client_filters_record_observation_by_topic() {
    let fixture = StoreFixture::new("topic-filter");
    fixture
        .client("(Record (workspace Decision \"workspace summary\" \"workspace context\" Maximum \"2026-05-19T13:08:11Z\" \"workspace quote\"))")
        .reply_text()
        .expect("workspace entry persisted");
    fixture
        .client("(Record (naming Correction \"naming summary\" \"naming context\" Maximum \"2026-05-19T13:12:00Z\" \"naming quote\"))")
        .reply_text()
        .expect("naming entry persisted");

    let reply = fixture
        .client("(Observe (Records ((Some naming) SummaryOnly)))")
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
        .client("(Record (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .reply_text()
        .expect("entry persisted");

    let reply = fixture
        .client("(Observe (Records (None WithProvenance)))")
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
        .client("(Record (naming Correction \"drop ancestor prefixes\" \"first context\" Maximum \"2026-05-19T13:08:11Z\" \"first wording\"))")
        .reply_text()
        .expect("first entry persisted");
    fixture
        .client("(Record (naming Correction \"drop ancestor prefixes\" \"second context\" Maximum \"2026-05-19T13:12:00Z\" \"second wording\"))")
        .reply_text()
        .expect("second entry persisted");

    let reply = fixture
        .client("(Observe (Records ((Some naming) SummaryOnly)))")
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
