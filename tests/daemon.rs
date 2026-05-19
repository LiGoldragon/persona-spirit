use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nota_codec::{Encoder, NotaEncode};
use persona_spirit::{
    DaemonConfiguration, DaemonRuntime, SingleArgument, SocketMode, SocketPath, SpiritClient,
    SpiritFrameCodec, SpiritSignalClient, StorePath,
};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Operation, Reply, Request,
    SessionEpoch, SignalVerb,
};
use signal_persona_spirit::{
    Certainty, Context, Entry, Frame, FrameBody, Kind, ObservationMode, Quote, RecordObservation,
    RecordQuery, SpiritReply, SpiritRequest, Summary, Timestamp, Topic,
};

#[derive(Debug, Clone)]
struct DaemonFixture {
    socket: SocketPath,
    store: StorePath,
}

impl DaemonFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut socket = std::env::temp_dir();
        socket.push(format!("persona-spirit-{test_name}-{nanos}.sock"));
        let mut store = std::env::temp_dir();
        store.push(format!("persona-spirit-{test_name}-{nanos}.redb"));
        Self {
            socket: SocketPath::new(socket.to_string_lossy().into_owned()),
            store: StorePath::new(store.to_string_lossy().into_owned()),
        }
    }

    fn configuration(&self) -> DaemonConfiguration {
        DaemonConfiguration::new(
            self.socket.clone(),
            self.store.clone(),
            SocketMode::from_octal(0o600),
        )
    }

    fn client(&self) -> SpiritSignalClient {
        SpiritSignalClient::new(self.socket.clone())
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
    SpiritRequest::RecordObservation(RecordObservation {
        query: RecordQuery {
            topic: None,
            mode: ObservationMode::SummaryOnly,
        },
    })
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
}

#[test]
fn persona_spirit_daemon_serves_signal_frames_through_actor_root() {
    let fixture = DaemonFixture::new("signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let socket = fixture.socket.clone();
    let handle = thread::spawn(move || daemon.serve_count(2));

    let client = fixture.client();
    let accepted = client
        .submit(SpiritRequest::Entry(entry("daemon accepted")))
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
        !socket.as_path().exists(),
        "daemon shutdown removes the socket path"
    );
}

#[test]
fn persona_spirit_daemon_rejects_verb_payload_mismatch_before_actor_execution() {
    let fixture = DaemonFixture::new("verb-mismatch");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let socket = fixture.socket.clone();
    let handle = thread::spawn(move || {
        let served = daemon.serve_one().expect("daemon serves rejected request");
        daemon.shutdown().expect("daemon shuts down");
        served
    });

    let codec = SpiritFrameCodec::default();
    let mut stream = UnixStream::connect(socket.as_path()).expect("client connects");
    let frame = Frame::new(FrameBody::Request {
        exchange: exchange(),
        request: Request::from_operations(NonEmpty::single(Operation::new(
            SignalVerb::Match,
            SpiritRequest::Entry(entry("wrong verb")),
        ))),
    });
    codec
        .write_frame(&mut stream, &frame)
        .expect("request frame writes");
    let reply_frame = codec.read_frame(&mut stream).expect("reply frame reads");
    let reply = codec
        .reply_from_frame(reply_frame)
        .expect("reply frame decodes");

    assert!(matches!(reply, Reply::Rejected { .. }));
    assert!(matches!(
        handle.join().expect("daemon thread exits").reply(),
        Reply::Rejected { .. }
    ));
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
        "(Entry (workspace Decision \"client socket\" \"daemon context\" Maximum \"2026-05-19T18:13:52Z\" \"daemon quote\"))"
            .to_string(),
    ])
    .expect("single request argument");

    let reply = SpiritClient::with_socket(argument, fixture.socket.clone())
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
