use std::fs;
use std::os::unix::net::UnixStream;
use std::process::Command;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nota_codec::{Encoder, NotaEncode};
use owner_signal_persona_spirit::{
    BootstrapPolicy, BootstrapPolicyReloaded, Frame as OwnerFrame, FrameBody as OwnerFrameBody,
    Generation, IdentityName, IdentityRegistered, Operation as OwnerOperation, Registration,
    Reply as OwnerReply, Start, Started,
};
use persona_spirit::{
    BootstrapPolicyPath, DaemonConfiguration, DaemonRuntime, SingleArgument, SocketMode,
    SocketPath, StorePath, ordinary, owner,
};
use signal_frame::{
    AcceptedOutcome, BatchFailureReason, CommitStatus, ExchangeIdentifier, ExchangeLane,
    LaneSequence, Reply, RequestBuilder, RequestPayload, RetryClassification, SessionEpoch,
    SubReply,
};
use signal_persona_spirit::{
    Certainty, Context, Entry, Frame, FrameBody, Kind, Observation, ObservationMode,
    Operation as WorkingOperation, Quote, RecordQuery, Reply as WorkingReply, Statement,
    StatementText, Summary, Topic,
};

#[derive(Debug, Clone)]
struct DaemonFixture {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    store: StorePath,
}

impl DaemonFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut socket = std::env::temp_dir();
        socket.push(format!("persona-spirit-{test_name}-{nanos}-ordinary.sock"));
        let mut owner_socket = std::env::temp_dir();
        owner_socket.push(format!("persona-spirit-{test_name}-{nanos}-owner.sock"));
        let mut store = std::env::temp_dir();
        store.push(format!("persona-spirit-{test_name}-{nanos}.redb"));
        Self {
            ordinary_socket: SocketPath::new(socket.to_string_lossy().into_owned()),
            owner_socket: SocketPath::new(owner_socket.to_string_lossy().into_owned()),
            store: StorePath::new(store.to_string_lossy().into_owned()),
        }
    }

    fn configuration(&self) -> DaemonConfiguration {
        DaemonConfiguration::new(
            self.ordinary_socket.clone(),
            self.owner_socket.clone(),
            self.store.clone(),
            SocketMode::from_octal(0o600),
        )
    }

    fn client(&self) -> ordinary::SignalClient {
        ordinary::SignalClient::new(self.ordinary_socket.clone())
    }

    fn owner_client(&self) -> owner::SignalClient {
        owner::SignalClient::new(self.owner_socket.clone())
    }
}

fn entry(summary: &str) -> Entry {
    Entry {
        topic: Topic::new("workspace"),
        kind: Kind::Decision,
        summary: Summary::new(summary),
        context: Context::new("daemon context"),
        certainty: Certainty::Maximum,
        quote: Quote::new("daemon quote"),
    }
}

fn observe_all() -> WorkingOperation {
    WorkingOperation::Observe(Observation::Records(RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::SummaryOnly,
    }))
}

fn observe_topics() -> WorkingOperation {
    WorkingOperation::Observe(Observation::Topics)
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[test]
fn persona_spirit_daemon_configuration_is_one_nota_record() {
    let fixture = DaemonFixture::new("configuration");
    let mut encoder = Encoder::new();
    fixture
        .configuration()
        .encode(&mut encoder)
        .expect("configuration encodes");
    let text = encoder.into_string();

    let configuration = DaemonConfiguration::from_text(&text).expect("configuration decodes");

    assert_eq!(configuration, fixture.configuration());
    assert!(
        text.starts_with('(') && !text.starts_with("(DaemonConfiguration"),
        "daemon configuration is a struct record, not a variant wrapper"
    );
    assert!(
        text.ends_with(" None)"),
        "daemon configuration carries an explicit optional bootstrap-policy path"
    );
}

#[test]
fn persona_spirit_daemon_serves_signal_frames_through_actor_root() {
    let fixture = DaemonFixture::new("signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let ordinary_socket = fixture.ordinary_socket.clone();
    let owner_socket = fixture.owner_socket.clone();
    let handle = thread::spawn(move || daemon.serve_count(2));

    let client = fixture.client();
    let accepted = client
        .submit(WorkingOperation::Record(entry("daemon accepted")))
        .expect("entry accepted through signal frame");
    assert_eq!(
        accepted,
        WorkingReply::RecordAccepted(signal_persona_spirit::RecordAccepted::new(
            signal_persona_spirit::RecordIdentifier::new(1)
        ))
    );

    let observed = client.submit(observe_all()).expect("records observed");
    assert_eq!(
        observed,
        WorkingReply::RecordsObserved(signal_persona_spirit::RecordsObserved {
            records: vec![signal_persona_spirit::RecordSummary {
                identifier: signal_persona_spirit::RecordIdentifier::new(1),
                topic: Topic::new("workspace"),
                kind: Kind::Decision,
                summary: Summary::new("daemon accepted"),
                certainty: Certainty::Maximum,
            }],
        })
    );

    let served = handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served two exchanges");
    assert_eq!(served.len(), 2);
    assert!(
        !ordinary_socket.as_path().exists(),
        "daemon shutdown removes the ordinary socket path"
    );
    assert!(
        !owner_socket.as_path().exists(),
        "daemon shutdown removes the owner socket path"
    );
}

#[test]
fn persona_spirit_daemon_rejects_multi_operation_batches_before_any_commit() {
    let fixture = DaemonFixture::new("multi-operation-batch");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let ordinary_socket = fixture.ordinary_socket.clone();
    let handle = thread::spawn(move || daemon.serve_count(2));

    let codec = ordinary::FrameCodec::default();
    let request = RequestBuilder::new()
        .with(WorkingOperation::Record(entry("first batch entry")))
        .with(WorkingOperation::Record(entry("second batch entry")))
        .build()
        .expect("non-empty multi operation request");
    let mut stream = UnixStream::connect(ordinary_socket.as_path()).expect("client connects");
    let frame = Frame::new(FrameBody::Request {
        exchange: exchange(),
        request,
    });
    codec
        .write_frame(&mut stream, &frame)
        .expect("multi operation request writes");
    let reply = codec
        .reply_from_frame(codec.read_frame(&mut stream).expect("reply frame reads"))
        .expect("reply decodes");

    assert_eq!(
        reply,
        Reply::Accepted {
            outcome: AcceptedOutcome::BatchAborted {
                reason: BatchFailureReason::EngineRejected,
                retry: RetryClassification::NotRetryable,
                commit: CommitStatus::NotCommitted,
            },
            per_operation: signal_frame::NonEmpty::from_head_and_tail(
                SubReply::Invalidated,
                vec![SubReply::Invalidated]
            ),
        }
    );

    let observed = fixture
        .client()
        .submit(observe_all())
        .expect("records observed");
    assert_eq!(
        observed,
        WorkingReply::RecordsObserved(signal_persona_spirit::RecordsObserved {
            records: Vec::new(),
        })
    );

    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served batch and observe frames");
}

#[test]
fn persona_spirit_daemon_classifies_state_frames_through_actor_root() {
    let fixture = DaemonFixture::new("signal-frame-state");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_count(2));

    let client = fixture.client();
    let accepted = client
        .submit(WorkingOperation::State(Statement {
            text: StatementText::new("daemon raw intent"),
        }))
        .expect("statement accepted through signal frame");
    assert_eq!(
        accepted,
        WorkingReply::RecordAccepted(signal_persona_spirit::RecordAccepted::new(
            signal_persona_spirit::RecordIdentifier::new(1)
        ))
    );

    let observed = client.submit(observe_all()).expect("records observed");
    assert_eq!(
        observed,
        WorkingReply::RecordsObserved(signal_persona_spirit::RecordsObserved {
            records: vec![signal_persona_spirit::RecordSummary {
                identifier: signal_persona_spirit::RecordIdentifier::new(1),
                topic: Topic::new("unclassified"),
                kind: Kind::Clarification,
                summary: Summary::new("daemon raw intent"),
                certainty: Certainty::Minimum,
            }],
        })
    );

    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served state and observe frames");
}

#[test]
fn persona_spirit_daemon_serves_topic_catalog_through_signal_frames() {
    let fixture = DaemonFixture::new("topic-catalog-signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_count(4));

    let client = fixture.client();
    client
        .submit(WorkingOperation::Record(entry("first spirit")))
        .expect("first entry accepted through signal frame");
    client
        .submit(WorkingOperation::Record(Entry {
            topic: Topic::new("naming"),
            kind: Kind::Correction,
            summary: Summary::new("naming entry"),
            context: Context::new("daemon context"),
            certainty: Certainty::Maximum,
            quote: Quote::new("daemon quote"),
        }))
        .expect("second entry accepted through signal frame");
    client
        .submit(WorkingOperation::Record(entry("second spirit")))
        .expect("third entry accepted through signal frame");

    let observed = client.submit(observe_topics()).expect("topics observed");
    assert_eq!(
        observed,
        WorkingReply::TopicsObserved(signal_persona_spirit::TopicsObserved {
            topics: vec![
                signal_persona_spirit::TopicCount {
                    topic: Topic::new("naming"),
                    entries: 1,
                },
                signal_persona_spirit::TopicCount {
                    topic: Topic::new("workspace"),
                    entries: 2,
                },
            ],
        })
    );

    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served record and observe frames");
}

#[test]
fn persona_spirit_daemon_serves_owner_signal_frames_through_owner_plane() {
    let fixture = DaemonFixture::new("owner-signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_owner_count(2));

    let client = fixture.owner_client();
    let started = client
        .submit(OwnerOperation::Start(Start {
            generation: Generation::new(7),
        }))
        .expect("owner start accepted through owner socket");
    assert_eq!(
        started,
        OwnerReply::Started(Started {
            generation: Generation::new(7),
        })
    );

    let registered = client
        .submit(OwnerOperation::Register(Registration {
            name: IdentityName::new("operator"),
        }))
        .expect("owner identity accepted through owner socket");
    assert_eq!(
        registered,
        OwnerReply::IdentityRegistered(IdentityRegistered {
            name: IdentityName::new("operator"),
        })
    );

    let served = handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served two owner exchanges");
    assert_eq!(served.len(), 2);
}

#[test]
fn persona_spirit_daemon_configuration_controls_bootstrap_policy_source() {
    let fixture = DaemonFixture::new("configured-policy");
    let mut policy_path = std::env::temp_dir();
    policy_path.push(format!(
        "persona-spirit-configured-policy-{}.nota",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    std::fs::write(&policy_path, "(\"daemon configured bootstrap policy\")")
        .expect("policy fixture writes");
    let configuration =
        fixture
            .configuration()
            .with_bootstrap_policy_path(BootstrapPolicyPath::new(
                policy_path.to_string_lossy().into_owned(),
            ));
    let daemon = DaemonRuntime::from_configuration(configuration)
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_owner_count(1));

    let reply = fixture
        .owner_client()
        .submit(OwnerOperation::Reload(BootstrapPolicy {}))
        .expect("configured policy reloads through owner socket");

    assert_eq!(
        reply,
        OwnerReply::BootstrapPolicyReloaded(BootstrapPolicyReloaded {})
    );
    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served owner reload");
    std::fs::remove_file(policy_path).expect("policy fixture removed");
}

#[test]
fn persona_spirit_ordinary_socket_rejects_owner_signal_frames() {
    let fixture = DaemonFixture::new("ordinary-rejects-owner");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let socket = fixture.ordinary_socket.clone();
    let handle = thread::spawn(move || {
        let served = daemon.serve_one();
        daemon.shutdown().expect("daemon shuts down");
        served
    });

    let codec = owner::FrameCodec::default();
    let mut stream = UnixStream::connect(socket.as_path()).expect("client connects");
    let frame = OwnerFrame::new(OwnerFrameBody::Request {
        exchange: exchange(),
        request: OwnerOperation::Start(Start {
            generation: Generation::new(1),
        })
        .into_request(),
    });
    codec
        .write_frame(&mut stream, &frame)
        .expect("owner request frame writes to ordinary socket");

    assert!(
        handle
            .join()
            .expect("daemon thread exits")
            .expect_err("ordinary socket rejects owner frame")
            .to_string()
            .contains("persona-spirit signal frame error")
    );
}

#[test]
fn persona_spirit_owner_socket_rejects_ordinary_signal_frames() {
    let fixture = DaemonFixture::new("owner-rejects-ordinary");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let socket = fixture.owner_socket.clone();
    let handle = thread::spawn(move || {
        let served = daemon.serve_owner_one();
        daemon.shutdown().expect("daemon shuts down");
        served
    });

    let codec = ordinary::FrameCodec::default();
    let mut stream = UnixStream::connect(socket.as_path()).expect("client connects");
    let frame = Frame::new(FrameBody::Request {
        exchange: exchange(),
        request: WorkingOperation::Record(entry("wrong socket")).into_request(),
    });
    codec
        .write_frame(&mut stream, &frame)
        .expect("ordinary request frame writes to owner socket");

    let error = handle
        .join()
        .expect("daemon thread exits")
        .expect_err("owner socket rejects ordinary frame")
        .to_string();
    assert!(
        error.contains("unexpected persona-spirit signal frame: expected owner request"),
        "unexpected owner-socket rejection error: {error}"
    );
}

#[test]
fn persona_spirit_daemon_source_does_not_route_signal_frames_through_nota_decoder() {
    let source = std::fs::read_to_string(format!("{}/src/daemon.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("daemon source is readable");

    assert!(!source.contains("NotaDecoder"));
    assert!(source.contains("SubmitFrameRequest"));
}

#[test]
fn persona_spirit_client_can_send_nota_request_to_running_daemon() {
    let fixture = DaemonFixture::new("client-socket");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_count(1));
    let argument = SingleArgument::from_arguments([
        "spirit".to_string(),
        "(Record (workspace Decision \"client socket\" \"daemon context\" Maximum \"daemon quote\"))"
            .to_string(),
    ])
    .expect("single request argument");

    let reply = ordinary::Client::with_socket(argument, fixture.ordinary_socket.clone())
        .reply_text()
        .expect("client sends to daemon");

    assert_eq!(reply, "(RecordAccepted 1)");
    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served client request");
}

#[test]
fn spirit_binary_can_send_request_file_to_running_daemon() {
    let fixture = DaemonFixture::new("spirit-binary-file");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_count(1));
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let mut request_path = std::env::temp_dir();
    request_path.push(format!("persona-spirit-cli-request-{nanos}.nota"));
    fs::write(
        &request_path,
        "(Record (workspace Decision \"binary file\" \"daemon context\" Maximum \"daemon quote\"))",
    )
    .expect("request file written");

    let output = Command::new(env!("CARGO_BIN_EXE_spirit"))
        .env("PERSONA_SPIRIT_SOCKET", fixture.ordinary_socket.as_path())
        .arg(&request_path)
        .output()
        .expect("spirit binary runs");

    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served client request");
    assert!(
        output.status.success(),
        "spirit stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "(RecordAccepted 1)"
    );
}

#[test]
fn spirit_binary_routes_owner_request_to_owner_socket() {
    let fixture = DaemonFixture::new("spirit-binary-owner");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_owner_count(1));

    let output = Command::new(env!("CARGO_BIN_EXE_spirit"))
        .env(
            "PERSONA_SPIRIT_OWNER_SOCKET",
            fixture.owner_socket.as_path(),
        )
        .env_remove("PERSONA_SPIRIT_SOCKET")
        .arg("(Register (operator))")
        .output()
        .expect("spirit binary runs");

    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served owner client request");
    assert!(
        output.status.success(),
        "spirit stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "(IdentityRegistered (operator))"
    );
}
