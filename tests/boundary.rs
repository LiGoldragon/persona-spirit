use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{
    DaemonConfiguration, DaemonRuntime, Error, SingleArgument, SocketMode, SocketPath,
    SpiritClient, SpiritRequestInput, SpiritRequestText, StorePath,
};

#[derive(Debug, Clone)]
struct StoreFixture {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    store: StorePath,
}

impl StoreFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut ordinary_socket = std::env::temp_dir();
        ordinary_socket.push(format!("persona-spirit-{test_name}-{nanos}-ordinary.sock"));
        let mut owner_socket = std::env::temp_dir();
        owner_socket.push(format!("persona-spirit-{test_name}-{nanos}-owner.sock"));
        let mut store = std::env::temp_dir();
        store.push(format!("persona-spirit-{test_name}-{nanos}.redb"));
        Self {
            ordinary_socket: SocketPath::new(ordinary_socket.to_string_lossy().into_owned()),
            owner_socket: SocketPath::new(owner_socket.to_string_lossy().into_owned()),
            store: StorePath::new(store.to_string_lossy().into_owned()),
        }
    }

    fn reply_text(&self, text: &str) -> persona_spirit::Result<String> {
        let argument = SingleArgument::from_arguments(["spirit".to_string(), text.to_string()])
            .expect("single argument accepted");
        let request_text = SpiritRequestInput::new(argument.clone()).text()?;
        SpiritRequestText::new(request_text).decode_request()?;
        let daemon = DaemonRuntime::from_configuration(DaemonConfiguration::new(
            self.ordinary_socket.clone(),
            self.owner_socket.clone(),
            self.store.clone(),
            SocketMode::from_octal(0o600),
        ))
        .bind()
        .expect("daemon binds");
        let handle = std::thread::spawn(move || daemon.serve_count(1));
        let reply = SpiritClient::with_socket(argument, self.ordinary_socket.clone()).reply_text();
        handle
            .join()
            .expect("daemon thread exits")
            .expect("daemon served request");
        reply
    }
}

#[test]
fn persona_spirit_binary_accepts_exactly_one_argument() {
    let argument = SingleArgument::from_arguments([
        "spirit".to_string(),
        "(State \"capture this intent\")".to_string(),
    ])
    .expect("single argument accepted");

    assert_eq!(argument.as_str(), "(State \"capture this intent\")");
}

#[test]
fn persona_spirit_binary_rejects_missing_argument() {
    let error = SingleArgument::from_arguments(["spirit".to_string()]).unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "spirit".to_string(),
            found: 0,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_extra_argument() {
    let error = SingleArgument::from_arguments([
        "spirit".to_string(),
        "(State \"one\")".to_string(),
        "(State \"two\")".to_string(),
    ])
    .unwrap_err();

    assert_eq!(
        error,
        Error::WrongArgumentCount {
            program: "spirit".to_string(),
            found: 2,
        }
    );
}

#[test]
fn persona_spirit_binary_rejects_flag_style_argument() {
    let error =
        SingleArgument::from_arguments(["spirit".to_string(), "--help".to_string()]).unwrap_err();

    assert_eq!(
        error,
        Error::FlagArgument {
            program: "spirit".to_string(),
            argument: "--help".to_string(),
        }
    );
}

#[test]
fn persona_spirit_binary_requires_socket_environment() {
    let output = Command::new(env!("CARGO_BIN_EXE_spirit"))
        .env_remove("PERSONA_SPIRIT_SOCKET")
        .arg("(Observe (State ()))")
        .output()
        .expect("binary runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("MissingSpiritSocket"));
}

#[test]
fn persona_spirit_client_accepts_request_file_path_argument() {
    let fixture = StoreFixture::new("request-file");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let mut request_path = std::env::temp_dir();
    request_path.push(format!("persona-spirit-request-{nanos}.nota"));
    fs::write(
        &request_path,
        "(Record (workspace Decision \"file request\" \"path context\" Maximum \"2026-05-20T15:42:00+02:00\" \"path quote\"))",
    )
    .expect("request file written");

    let reply = fixture
        .reply_text(&request_path.to_string_lossy())
        .expect("file path request persisted");

    assert_eq!(
        reply,
        "(RecordAccepted ((1 workspace Decision \"file request\" Maximum)))"
    );
}

#[test]
fn persona_spirit_client_classifies_statement_as_provisional_record() {
    let fixture = StoreFixture::new("statement");
    let reply = fixture
        .reply_text("(State (\"capture this intent\"))")
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
        .reply_text("(Record (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
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
        .reply_text("(Record (workspace Decision \"first summary\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .expect("first entry persisted");
    fixture
        .reply_text("(Record (workspace Correction \"second summary\" \"current implementation context\" Medium \"2026-05-19T13:12:00Z\" \"second statement\"))")
        .expect("second entry persisted");

    let reply = fixture
        .reply_text("(Observe (Records (None SummaryOnly)))")
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
        .reply_text("(Observe (State ()))")
        .expect("state observed");

    assert_eq!(reply, "(StateObserved ((Absent None)))");
}

#[test]
fn persona_spirit_client_observes_empty_pending_questions() {
    let fixture = StoreFixture::new("question-observation");
    let reply = fixture
        .reply_text("(Observe (Questions ()))")
        .expect("questions observed");

    assert_eq!(reply, "(QuestionsObserved ([]))");
}

#[test]
fn persona_spirit_client_opens_and_retracts_state_subscription() {
    let fixture = StoreFixture::new("state-subscription");
    let opened = fixture
        .reply_text("(Watch (State ()))")
        .expect("state subscription opened");
    let retracted = fixture
        .reply_text("(Unwatch (State (1)))")
        .expect("state subscription retracted");

    assert_eq!(opened, "(StateSubscriptionOpened ((1) (Absent None)))");
    assert_eq!(retracted, "(StateSubscriptionRetracted ((1)))");
}

#[test]
fn persona_spirit_client_opens_record_subscription_with_summary_snapshot() {
    let fixture = StoreFixture::new("record-subscription");
    fixture
        .reply_text("(Record (workspace Decision \"subscription summary\" \"workspace context\" Maximum \"2026-05-19T13:08:11Z\" \"workspace quote\"))")
        .expect("entry persisted");

    let opened = fixture
        .reply_text("(Watch (Records (None SummaryOnly)))")
        .expect("record subscription opened");
    let retracted = fixture
        .reply_text("(Unwatch (Records (1)))")
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
        .reply_text("(Record (workspace Decision \"workspace summary\" \"workspace context\" Maximum \"2026-05-19T13:08:11Z\" \"workspace quote\"))")
        .expect("workspace entry persisted");
    fixture
        .reply_text("(Record (naming Correction \"naming summary\" \"naming context\" Maximum \"2026-05-19T13:12:00Z\" \"naming quote\"))")
        .expect("naming entry persisted");

    let reply = fixture
        .reply_text("(Observe (Records ((Some naming) SummaryOnly)))")
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
        .reply_text("(Record (workspace Decision \"summary only\" \"current implementation context\" Maximum \"2026-05-19T13:08:11Z\" \"first statement\"))")
        .expect("entry persisted");

    let reply = fixture
        .reply_text("(Observe (Records (None WithProvenance)))")
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
        .reply_text("(Record (naming Correction \"drop ancestor prefixes\" \"first context\" Maximum \"2026-05-19T13:08:11Z\" \"first wording\"))")
        .expect("first entry persisted");
    fixture
        .reply_text("(Record (naming Correction \"drop ancestor prefixes\" \"second context\" Maximum \"2026-05-19T13:12:00Z\" \"second wording\"))")
        .expect("second entry persisted");

    let reply = fixture
        .reply_text("(Observe (Records ((Some naming) SummaryOnly)))")
        .expect("records observed");

    assert_eq!(
        reply,
        "(RecordsObserved ([(1 naming Correction \"drop ancestor prefixes\" Maximum) (2 naming Correction \"drop ancestor prefixes\" Maximum)]))"
    );
}

#[test]
fn persona_spirit_client_rejects_unknown_record_shape() {
    let fixture = StoreFixture::new("unknown-record");
    let error = fixture.reply_text("(UnknownIntent workspace)").unwrap_err();

    assert!(matches!(error, Error::InvalidSpiritRequest { .. }));
}
