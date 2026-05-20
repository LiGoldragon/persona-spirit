use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nota_codec::{Encoder, NotaEncode};
use owner_signal_persona_spirit::{
    Frame as OwnerFrame, FrameBody as OwnerFrameBody, Generation, IdentityName, IdentityRegistered,
    OwnerSpiritReply, OwnerSpiritRequest, RegisterIdentity, ReloadBootstrapPolicyOrder, StartOrder,
    Started,
};
use persona_spirit::{
    BootstrapPolicyPath, DaemonConfiguration, DaemonRuntime, OwnerSpiritFrameCodec,
    OwnerSpiritSignalClient, SingleArgument, SocketMode, SocketPath, SpiritClient,
    SpiritFrameCodec, SpiritSignalClient, StorePath,
};
use signal_core::{
    ExchangeIdentifier as OwnerExchangeIdentifier, ExchangeLane as OwnerExchangeLane,
    LaneSequence as OwnerLaneSequence, RequestPayload as OwnerRequestPayload,
    SessionEpoch as OwnerSessionEpoch,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, RequestPayload, SessionEpoch};
use signal_persona_spirit::{
    Certainty, Context, Entry, Frame, FrameBody, Kind, Observation, ObservationMode, Quote,
    RecordQuery, SpiritReply, SpiritRequest, Summary, Timestamp, Topic,
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

    fn client(&self) -> SpiritSignalClient {
        SpiritSignalClient::new(self.ordinary_socket.clone())
    }

    fn owner_client(&self) -> OwnerSpiritSignalClient {
        OwnerSpiritSignalClient::new(self.owner_socket.clone())
    }
}

fn entry(summary: &str) -> Entry {
    Entry {
        topic: Topic::new("workspace"),
        kind: Kind::Decision,
        summary: Summary::new(summary),
        context: Context::new("daemon context"),
        certainty: Certainty::Maximum,
        timestamp: Timestamp::new("2026-05-19T18:13:52Z"),
        quote: Quote::new("daemon quote"),
    }
}

fn observe_all() -> SpiritRequest {
    SpiritRequest::Observe(Observation::Records(RecordQuery {
        topic: None,
        mode: ObservationMode::SummaryOnly,
    }))
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn owner_exchange() -> OwnerExchangeIdentifier {
    OwnerExchangeIdentifier::new(
        OwnerSessionEpoch::new(0),
        OwnerExchangeLane::Connector,
        OwnerLaneSequence::first(),
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
        .submit(SpiritRequest::Record(entry("daemon accepted")))
        .expect("entry accepted through signal frame");
    assert_eq!(
        accepted,
        SpiritReply::RecordAccepted(signal_persona_spirit::RecordAccepted {
            captured: signal_persona_spirit::RecordSummary {
                identifier: signal_persona_spirit::RecordIdentifier::new(1),
                topic: Topic::new("workspace"),
                kind: Kind::Decision,
                summary: Summary::new("daemon accepted"),
                certainty: Certainty::Maximum,
            },
        })
    );

    let observed = client.submit(observe_all()).expect("records observed");
    assert_eq!(
        observed,
        SpiritReply::RecordsObserved(signal_persona_spirit::RecordsObserved {
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
fn persona_spirit_daemon_serves_owner_signal_frames_through_owner_plane() {
    let fixture = DaemonFixture::new("owner-signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_owner_count(2));

    let client = fixture.owner_client();
    let started = client
        .submit(OwnerSpiritRequest::StartOrder(StartOrder {
            generation: Generation::new(7),
        }))
        .expect("owner start accepted through owner socket");
    assert_eq!(
        started,
        OwnerSpiritReply::Started(Started {
            generation: Generation::new(7),
        })
    );

    let registered = client
        .submit(OwnerSpiritRequest::RegisterIdentity(RegisterIdentity {
            name: IdentityName::new("operator"),
        }))
        .expect("owner identity accepted through owner socket");
    assert_eq!(
        registered,
        OwnerSpiritReply::IdentityRegistered(IdentityRegistered {
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
        .submit(OwnerSpiritRequest::ReloadBootstrapPolicyOrder(
            ReloadBootstrapPolicyOrder {},
        ))
        .expect("configured policy reloads through owner socket");

    assert_eq!(
        reply,
        OwnerSpiritReply::BootstrapPolicyReloaded(
            owner_signal_persona_spirit::BootstrapPolicyReloaded {}
        )
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

    let codec = OwnerSpiritFrameCodec::default();
    let mut stream = UnixStream::connect(socket.as_path()).expect("client connects");
    let frame = OwnerFrame::new(OwnerFrameBody::Request {
        exchange: owner_exchange(),
        request: OwnerSpiritRequest::StartOrder(StartOrder {
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

    let codec = SpiritFrameCodec::default();
    let mut stream = UnixStream::connect(socket.as_path()).expect("client connects");
    let frame = Frame::new(FrameBody::Request {
        exchange: exchange(),
        request: SpiritRequest::Record(entry("wrong socket")).into_request(),
    });
    codec
        .write_frame(&mut stream, &frame)
        .expect("ordinary request frame writes to owner socket");

    assert!(
        handle
            .join()
            .expect("daemon thread exits")
            .expect_err("owner socket rejects ordinary frame")
            .to_string()
            .contains("persona-spirit signal frame error")
    );
}

#[test]
fn persona_spirit_daemon_source_does_not_route_signal_frames_through_nota_decoder() {
    let source = std::fs::read_to_string(format!("{}/src/daemon.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("daemon source is readable");

    assert!(!source.contains("NotaDecoder"));
    assert!(source.contains("SubmitRequest"));
}

#[test]
fn persona_spirit_client_can_send_nota_request_to_running_daemon() {
    let fixture = DaemonFixture::new("client-socket");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let handle = thread::spawn(move || daemon.serve_count(1));
    let argument = SingleArgument::from_arguments([
        "persona-spirit".to_string(),
        "(Record (workspace Decision \"client socket\" \"daemon context\" Maximum \"2026-05-19T18:13:52Z\" \"daemon quote\"))"
            .to_string(),
    ])
    .expect("single request argument");

    let reply = SpiritClient::with_socket(argument, fixture.ordinary_socket.clone())
        .reply_text()
        .expect("client sends to daemon");

    assert_eq!(
        reply,
        "(RecordAccepted ((1 workspace Decision \"client socket\" Maximum)))"
    );
    handle
        .join()
        .expect("daemon thread exits")
        .expect("daemon served client request");
}
