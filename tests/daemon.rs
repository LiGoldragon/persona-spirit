use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
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
    SocketPath, StorePath, ordinary, owner, store::StampedEntry, upgrade,
};
use signal_frame::{
    AcceptedOutcome, BatchFailureReason, ClientShape, CommandLineSockets, CommitStatus,
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestBuilder,
    RequestPayload, RetryClassification, SessionEpoch, SubReply,
};
use signal_persona::engine_management::{
    Frame as EngineManagementFrame, FrameBody as EngineManagementFrameBody,
    Operation as EngineManagementOperation, Query as EngineManagementQuery,
    Reply as EngineManagementReply,
};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName as EngineManagementComponentName,
    EngineManagementProtocolVersion, Presence,
};
use signal_persona_spirit::{
    Context, Date, Entry, Frame, FrameBody, Kind, Observation, ObservationMode,
    Operation as WorkingOperation, Quote, RecordQuery, Reply as WorkingReply, Statement,
    StatementText, Summary, Time, Topic,
};
use signal_sema::Magnitude;
use signal_version_handover::{
    HandoverAcceptance, HandoverFinalization, HandoverRejection, HandoverRejectionReason,
    MarkerRequest, Operation as UpgradeOperation, Reply as UpgradeReply,
};
use unix_ancillary::UnixStreamExt;
use version_projection::{ComponentName, ContractVersion, Projected, RecordKind};

#[derive(Debug, Clone)]
struct DaemonFixture {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    upgrade_socket: SocketPath,
    engine_management_socket: SocketPath,
    handoff_control_socket: SocketPath,
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
        let mut upgrade_socket = std::env::temp_dir();
        upgrade_socket.push(format!("persona-spirit-{test_name}-{nanos}-upgrade.sock"));
        let mut engine_management_socket = std::env::temp_dir();
        engine_management_socket.push(format!(
            "persona-spirit-{test_name}-{nanos}-engine-management.sock"
        ));
        let mut handoff_control_socket = std::env::temp_dir();
        handoff_control_socket.push(format!(
            "persona-spirit-{test_name}-{nanos}-handoff-control.sock"
        ));
        let mut store = std::env::temp_dir();
        store.push(format!("persona-spirit-{test_name}-{nanos}.redb"));
        Self {
            ordinary_socket: SocketPath::new(socket.to_string_lossy().into_owned()),
            owner_socket: SocketPath::new(owner_socket.to_string_lossy().into_owned()),
            upgrade_socket: SocketPath::new(upgrade_socket.to_string_lossy().into_owned()),
            engine_management_socket: SocketPath::new(
                engine_management_socket.to_string_lossy().into_owned(),
            ),
            handoff_control_socket: SocketPath::new(
                handoff_control_socket.to_string_lossy().into_owned(),
            ),
            store: StorePath::new(store.to_string_lossy().into_owned()),
        }
    }

    fn configuration(&self) -> DaemonConfiguration {
        DaemonConfiguration::new(
            self.ordinary_socket.clone(),
            self.owner_socket.clone(),
            self.upgrade_socket.clone(),
            self.store.clone(),
            SocketMode::from_octal(0o600),
        )
    }

    fn configuration_with_engine_management(&self) -> DaemonConfiguration {
        self.configuration().with_engine_management_socket_path(
            self.engine_management_socket.clone(),
            SocketMode::from_octal(0o600),
        )
    }

    fn client(&self) -> ordinary::SignalClient {
        ordinary::SignalClient::new(self.ordinary_socket.clone())
    }

    fn owner_client(&self) -> owner::SignalClient {
        owner::SignalClient::new(self.owner_socket.clone())
    }

    fn upgrade_client(&self) -> upgrade::SignalClient {
        upgrade::SignalClient::new(self.upgrade_socket.clone())
    }

    fn handoff_control_listener(&self) -> UnixListener {
        UnixListener::bind(self.handoff_control_socket.as_path())
            .expect("handoff control listener binds")
    }
}

fn entry(summary: &str) -> Entry {
    Entry {
        topic: Topic::new("workspace"),
        kind: Kind::Decision,
        summary: Summary::new(summary),
        context: Context::new("daemon context"),
        certainty: Magnitude::Maximum,
        quote: Quote::new("daemon quote"),
    }
}

fn mirrored_stamped_entry_payload(summary: &str) -> Vec<u8> {
    let entry = StampedEntry::new(entry(summary), Date::new(2026, 5, 22), Time::new(20, 52, 0));
    rkyv::to_bytes::<rkyv::rancor::Error>(&entry)
        .expect("stamped entry encodes")
        .as_ref()
        .to_vec()
}

fn observe_all() -> WorkingOperation {
    WorkingOperation::Observe(Observation::Records(RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::SummaryOnly,
    }))
}

fn engine_management_request_frame(operation: EngineManagementOperation) -> EngineManagementFrame {
    EngineManagementFrame::new(EngineManagementFrameBody::Request {
        exchange: ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        ),
        request: signal_frame::Request::from_payload(operation),
    })
}

fn write_engine_management_request(stream: &mut UnixStream, operation: EngineManagementOperation) {
    let bytes = engine_management_request_frame(operation)
        .encode_length_prefixed()
        .expect("engine management request encodes");
    stream
        .write_all(&bytes)
        .expect("engine management request writes");
    stream.flush().expect("engine management request flushes");
}

fn read_engine_management_reply(stream: &mut UnixStream) -> EngineManagementReply {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .expect("engine management reply length reads");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream
        .read_exact(&mut bytes[4..])
        .expect("engine management reply body reads");
    let frame =
        EngineManagementFrame::decode_length_prefixed(&bytes).expect("engine management decodes");
    match frame.into_body() {
        EngineManagementFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => payload,
                other => panic!("expected engine management reply payload, got {other:?}"),
            },
            other => panic!("expected accepted engine management reply, got {other:?}"),
        },
        other => panic!("expected engine management reply frame, got {other:?}"),
    }
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
        text.ends_with(" None None None None)"),
        "daemon configuration carries explicit optional bootstrap-policy, handoff control, and engine-management paths"
    );
}

#[test]
fn persona_spirit_daemon_accepts_configuration_file_path_argument() {
    let fixture = DaemonFixture::new("configuration-file-path");
    let mut encoder = Encoder::new();
    fixture
        .configuration()
        .encode(&mut encoder)
        .expect("configuration encodes");
    let mut path = std::env::temp_dir();
    path.push(format!(
        "persona-spirit-configuration-file-path-{}.nota",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    fs::write(&path, encoder.into_string()).expect("configuration file written");

    let argument = SingleArgument::from_arguments([
        "persona-spirit-daemon".to_string(),
        path.to_string_lossy().into_owned(),
    ])
    .expect("single configuration path accepted");

    DaemonRuntime::from_argument(argument).expect("daemon loads configuration file path");
}

#[test]
fn persona_spirit_daemon_serves_engine_management_socket_for_supervision() {
    let fixture = DaemonFixture::new("engine-management");
    let mut daemon =
        DaemonRuntime::from_configuration(fixture.configuration_with_engine_management())
            .bind()
            .expect("daemon binds with engine-management socket");
    let socket = fixture.engine_management_socket.clone();
    let handle = thread::spawn(move || {
        let served = daemon.serve_engine_management_one()?;
        daemon.shutdown()?;
        Ok::<_, persona_spirit::Error>(served)
    });

    let mut stream =
        UnixStream::connect(socket.as_path()).expect("engine-management client connects");
    write_engine_management_request(
        &mut stream,
        EngineManagementOperation::Announce(Presence {
            expected_component: EngineManagementComponentName::new("persona-spirit"),
            expected_kind: ComponentKind::Spirit,
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    let identity = read_engine_management_reply(&mut stream);
    match identity {
        EngineManagementReply::Identified(identity) => {
            assert_eq!(
                identity.name,
                EngineManagementComponentName::new("persona-spirit")
            );
            assert_eq!(identity.kind, ComponentKind::Spirit);
            assert_eq!(
                identity.engine_management_protocol_version,
                EngineManagementProtocolVersion::new(1)
            );
        }
        other => panic!("expected identified reply, got {other:?}"),
    }

    write_engine_management_request(
        &mut stream,
        EngineManagementOperation::Query(EngineManagementQuery::ReadinessStatus(
            EngineManagementComponentName::new("persona-spirit"),
        )),
    );
    assert!(matches!(
        read_engine_management_reply(&mut stream),
        EngineManagementReply::Ready(_)
    ));

    write_engine_management_request(
        &mut stream,
        EngineManagementOperation::Query(EngineManagementQuery::HealthStatus(
            EngineManagementComponentName::new("persona-spirit"),
        )),
    );
    match read_engine_management_reply(&mut stream) {
        EngineManagementReply::HealthReport(report) => {
            assert_eq!(report.health, ComponentHealth::Running);
        }
        other => panic!("expected health report, got {other:?}"),
    }

    drop(stream);
    let served = handle
        .join()
        .expect("engine-management server thread joins")
        .expect("engine-management exchange succeeds");
    assert_eq!(served.len(), 3);
}

#[test]
fn persona_spirit_daemon_serves_signal_frames_through_actor_root() {
    let fixture = DaemonFixture::new("signal-frame");
    let daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let ordinary_socket = fixture.ordinary_socket.clone();
    let owner_socket = fixture.owner_socket.clone();
    let upgrade_socket = fixture.upgrade_socket.clone();
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
                certainty: Magnitude::Maximum,
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
    assert!(
        !upgrade_socket.as_path().exists(),
        "daemon shutdown removes the upgrade socket path"
    );
}

#[test]
fn persona_spirit_daemon_serves_signal_frames_from_handed_off_file_descriptor() {
    let fixture = DaemonFixture::new("handoff-control");
    let control_listener = fixture.handoff_control_listener();
    let mut daemon = DaemonRuntime::from_configuration(
        fixture
            .configuration()
            .with_handoff_control_socket_path(fixture.handoff_control_socket.clone()),
    )
    .bind()
    .expect("daemon binds");
    let (persona_control, _address) = control_listener
        .accept()
        .expect("daemon connects to Persona control socket");

    let codec = ordinary::FrameCodec::default();
    let (mut client_stream, daemon_stream) =
        UnixStream::pair().expect("client and handed-off stream pair");
    persona_control
        .send_fds(b"spirit-public-fd", &[&daemon_stream])
        .expect("Persona sends accepted client fd");
    drop(daemon_stream);

    let client_handle = thread::spawn(move || {
        let frame = codec.request_frame(WorkingOperation::Record(entry("handoff accepted")));
        codec
            .write_frame(&mut client_stream, &frame)
            .expect("handoff request writes");
        codec
            .reply_from_frame(codec.read_frame(&mut client_stream).expect("reply reads"))
            .expect("reply decodes")
    });

    let served = daemon
        .serve_handoff_one()
        .expect("daemon serves handed-off stream");
    let reply = client_handle.join().expect("client exits");
    assert_eq!(served.reply(), &reply);
    assert_eq!(
        reply,
        Reply::committed(NonEmpty::single(SubReply::Ok(
            WorkingReply::RecordAccepted(signal_persona_spirit::RecordAccepted::new(
                signal_persona_spirit::RecordIdentifier::new(1)
            ))
        )))
    );

    daemon.shutdown().expect("daemon shuts down");
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
                certainty: Magnitude::Minimum,
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
            certainty: Magnitude::Maximum,
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
fn persona_spirit_daemon_serves_version_handover_frames_through_upgrade_socket() {
    let fixture = DaemonFixture::new("upgrade-signal-frame");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    let served_marker = daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    assert_eq!(
        served_marker.reply(),
        &Reply::committed(NonEmpty::single(SubReply::Ok(marker_reply.clone())))
    );
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };
    assert_eq!(marker.component, component);
    assert_eq!(marker.commit_sequence, 0);
    assert_eq!(marker.write_counter, 0);
    assert_eq!(marker.last_record_identifier, None);

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    let UpgradeReply::HandoverAccepted(HandoverAcceptance { accepted_marker }) = readiness_reply
    else {
        panic!("expected handover accepted, got {readiness_reply:?}");
    };
    assert_eq!(accepted_marker.component, marker.component);
    assert_eq!(accepted_marker.commit_sequence, marker.commit_sequence);
    assert_eq!(accepted_marker.write_counter, marker.write_counter);
    assert_eq!(
        accepted_marker.last_record_identifier,
        marker.last_record_identifier
    );

    let completion_client = client.clone();
    let component_for_completion = component.clone();
    let marker_for_completion = accepted_marker.clone();
    let completion_handle = thread::spawn(move || {
        completion_client.submit(UpgradeOperation::HandoverCompleted(
            signal_version_handover::CompletionReport {
                component: component_for_completion,
                accepted_marker: marker_for_completion,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves completion exchange");
    let completion_reply = completion_handle
        .join()
        .expect("completion client exits")
        .expect("completion reply received");
    assert_eq!(
        completion_reply,
        UpgradeReply::HandoverFinalized(HandoverFinalization {
            finalized_marker: accepted_marker,
        })
    );

    assert!(
        !fixture.ordinary_socket.as_path().exists(),
        "ordinary socket path is removed after handover completion"
    );
    assert!(
        !fixture.owner_socket.as_path().exists(),
        "owner socket path is removed after handover completion"
    );
    let ordinary_error = fixture
        .client()
        .submit(WorkingOperation::Record(entry("after handover")))
        .expect_err("ordinary socket is closed after handover");
    assert!(
        ordinary_error.to_string().contains("No such file")
            || ordinary_error.to_string().contains("not found"),
        "unexpected ordinary close error: {ordinary_error}"
    );

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_completion_requires_accepted_readiness() {
    let fixture = DaemonFixture::new("upgrade-completion-requires-readiness");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let completion_client = client.clone();
    let component_for_completion = component.clone();
    let completion_handle = thread::spawn(move || {
        completion_client.submit(UpgradeOperation::HandoverCompleted(
            signal_version_handover::CompletionReport {
                component: component_for_completion,
                accepted_marker: marker,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves completion exchange");
    let completion_reply = completion_handle
        .join()
        .expect("completion client exits")
        .expect("completion reply received");
    assert_eq!(
        completion_reply,
        UpgradeReply::HandoverRejected(HandoverRejection {
            component: component.clone(),
            reason: HandoverRejectionReason::NotReady,
        })
    );

    assert!(
        fixture.ordinary_socket.as_path().exists(),
        "ordinary socket remains after rejected completion"
    );
    let ordinary_client = fixture.client();
    let ordinary_handle = thread::spawn(move || {
        ordinary_client.submit(WorkingOperation::Record(entry("completion rejected")))
    });
    daemon
        .serve_one()
        .expect("daemon still serves ordinary exchange");
    let ordinary_reply = ordinary_handle
        .join()
        .expect("ordinary client exits")
        .expect("ordinary reply received");
    assert_eq!(
        ordinary_reply,
        WorkingReply::RecordAccepted(signal_persona_spirit::RecordAccepted::new(
            signal_persona_spirit::RecordIdentifier::new(1)
        ))
    );

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_readiness_rejects_commit_sequence_drift() {
    let fixture = DaemonFixture::new("upgrade-readiness-rejects-drift");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let ordinary_client = fixture.client();
    let ordinary_handle = thread::spawn(move || {
        ordinary_client.submit(WorkingOperation::Record(entry("drift before readiness")))
    });
    daemon
        .serve_one()
        .expect("daemon serves ordinary exchange that advances commit sequence");
    let ordinary_reply = ordinary_handle
        .join()
        .expect("ordinary client exits")
        .expect("ordinary reply received");
    assert_eq!(
        ordinary_reply,
        WorkingReply::RecordAccepted(signal_persona_spirit::RecordAccepted::new(
            signal_persona_spirit::RecordIdentifier::new(1)
        ))
    );

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    assert_eq!(
        readiness_reply,
        UpgradeReply::HandoverRejected(HandoverRejection {
            component,
            reason: HandoverRejectionReason::CommitSequenceAdvanced,
        })
    );
    assert!(
        fixture.ordinary_socket.as_path().exists(),
        "ordinary socket remains after drift rejection"
    );

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_readiness_freezes_public_writes_until_completion() {
    let fixture = DaemonFixture::new("upgrade-readiness-freezes-writes");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    let UpgradeReply::HandoverAccepted(HandoverAcceptance { accepted_marker }) = readiness_reply
    else {
        panic!("expected handover accepted, got {readiness_reply:?}");
    };

    let write_client = fixture.client();
    let write_handle = thread::spawn(move || {
        write_client.submit(WorkingOperation::Record(entry("write after readiness")))
    });
    daemon
        .serve_one()
        .expect("daemon serves rejected ordinary write");
    let write_error = write_handle
        .join()
        .expect("write client exits")
        .expect_err("ordinary write is rejected during handover mode");
    assert!(
        write_error
            .to_string()
            .contains("persona-spirit request rejected before execution: receiver-internal"),
        "unexpected handover-mode write error: {write_error}"
    );

    let read_client = fixture.client();
    let read_handle =
        thread::spawn(move || read_client.submit(WorkingOperation::Observe(Observation::Topics)));
    daemon
        .serve_one()
        .expect("daemon serves ordinary read during handover mode");
    let read_reply = read_handle
        .join()
        .expect("read client exits")
        .expect("ordinary read remains available during handover mode");
    assert_eq!(
        read_reply,
        WorkingReply::TopicsObserved(signal_persona_spirit::TopicsObserved { topics: vec![] })
    );

    let completion_client = client.clone();
    let component_for_completion = component.clone();
    let marker_for_completion = accepted_marker.clone();
    let completion_handle = thread::spawn(move || {
        completion_client.submit(UpgradeOperation::HandoverCompleted(
            signal_version_handover::CompletionReport {
                component: component_for_completion,
                accepted_marker: marker_for_completion,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves completion exchange");
    let completion_reply = completion_handle
        .join()
        .expect("completion client exits")
        .expect("completion reply received");
    assert_eq!(
        completion_reply,
        UpgradeReply::HandoverFinalized(HandoverFinalization {
            finalized_marker: accepted_marker,
        })
    );

    assert!(
        !fixture.ordinary_socket.as_path().exists(),
        "ordinary socket path is removed after handover completion"
    );
    assert!(
        !fixture.owner_socket.as_path().exists(),
        "owner socket path is removed after handover completion"
    );

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_recovery_reopens_public_writes_after_readiness() {
    let fixture = DaemonFixture::new("upgrade-recovery-reopens-writes");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    assert!(matches!(readiness_reply, UpgradeReply::HandoverAccepted(_)));

    let frozen_client = fixture.client();
    let frozen_write = thread::spawn(move || {
        frozen_client.submit(WorkingOperation::Record(entry("write while frozen")))
    });
    daemon
        .serve_one()
        .expect("daemon serves rejected ordinary write");
    frozen_write
        .join()
        .expect("frozen write client exits")
        .expect_err("ordinary write is rejected before recovery");

    let recovery_client = client.clone();
    let component_for_recovery = component.clone();
    let recovery_handle = thread::spawn(move || {
        recovery_client.submit(UpgradeOperation::RecoverFromFailure(
            signal_version_handover::RecoveryRequest {
                component: component_for_recovery,
                failure_identifier: marker.commit_sequence,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves recovery exchange");
    let recovery_reply = recovery_handle
        .join()
        .expect("recovery client exits")
        .expect("recovery reply received");
    assert_eq!(
        recovery_reply,
        UpgradeReply::RecoveryCompleted(signal_version_handover::RecoveryResult {
            component,
            recovered: true,
        })
    );

    let recovered_client = fixture.client();
    let recovered_write = thread::spawn(move || {
        recovered_client.submit(WorkingOperation::Record(entry("write after recovery")))
    });
    daemon
        .serve_one()
        .expect("daemon serves recovered ordinary write");
    recovered_write
        .join()
        .expect("recovered write client exits")
        .expect("ordinary write is accepted after recovery");

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_mirror_applies_stamped_entry_after_completion() {
    let fixture = DaemonFixture::new("upgrade-mirror-after-completion");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    let UpgradeReply::HandoverAccepted(HandoverAcceptance { accepted_marker }) = readiness_reply
    else {
        panic!("expected handover accepted, got {readiness_reply:?}");
    };
    assert_eq!(accepted_marker.component, marker.component);
    assert_eq!(accepted_marker.commit_sequence, marker.commit_sequence);
    assert_eq!(accepted_marker.write_counter, marker.write_counter);
    assert_eq!(
        accepted_marker.last_record_identifier,
        marker.last_record_identifier
    );

    let completion_client = client.clone();
    let component_for_completion = component.clone();
    let marker_for_completion = accepted_marker.clone();
    let completion_handle = thread::spawn(move || {
        completion_client.submit(UpgradeOperation::HandoverCompleted(
            signal_version_handover::CompletionReport {
                component: component_for_completion,
                accepted_marker: marker_for_completion,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves completion exchange");
    let completion_reply = completion_handle
        .join()
        .expect("completion client exits")
        .expect("completion reply received");
    assert_eq!(
        completion_reply,
        UpgradeReply::HandoverFinalized(HandoverFinalization {
            finalized_marker: accepted_marker,
        })
    );

    let mirror_client = client.clone();
    let component_for_mirror = component.clone();
    let mirror_handle = thread::spawn(move || {
        mirror_client.submit(UpgradeOperation::Mirror(
            signal_version_handover::MirrorPayload {
                component: component_for_mirror,
                source_version: ContractVersion::new([2; 32]),
                target_version: <StampedEntry as Projected>::CONTRACT_VERSION,
                kind: RecordKind::new("StampedEntry"),
                payload: mirrored_stamped_entry_payload("mirrored after completion"),
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves mirror exchange");
    let mirror_reply = mirror_handle
        .join()
        .expect("mirror client exits")
        .expect("mirror reply received");
    assert_eq!(
        mirror_reply,
        UpgradeReply::MirrorAcknowledged(signal_version_handover::MirrorAcknowledgement {
            component: component.clone(),
            write_counter: 1,
        })
    );

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves post-mirror marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected post-mirror marker, got {marker_reply:?}");
    };
    assert_eq!(marker.commit_sequence, 1);
    assert_eq!(marker.write_counter, 1);
    assert_eq!(marker.last_record_identifier, Some(1));
    assert!(
        !fixture.ordinary_socket.as_path().exists(),
        "ordinary socket remains closed while mirror uses private upgrade socket"
    );

    daemon.shutdown().expect("daemon shuts down");
}

#[test]
fn persona_spirit_upgrade_mirror_rejects_wrong_target_version() {
    let fixture = DaemonFixture::new("upgrade-mirror-wrong-target");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("daemon binds");
    let client = fixture.upgrade_client();
    let component = ComponentName::new("persona-spirit");

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected handover marker, got {marker_reply:?}");
    };

    let readiness_client = client.clone();
    let component_for_readiness = component.clone();
    let marker_for_readiness = marker.clone();
    let readiness_handle = thread::spawn(move || {
        readiness_client.submit(UpgradeOperation::ReadyToHandover(
            signal_version_handover::ReadinessReport {
                component: component_for_readiness,
                source_marker: marker_for_readiness,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves readiness exchange");
    let readiness_reply = readiness_handle
        .join()
        .expect("readiness client exits")
        .expect("readiness reply received");
    let UpgradeReply::HandoverAccepted(HandoverAcceptance { accepted_marker }) = readiness_reply
    else {
        panic!("expected handover accepted, got {readiness_reply:?}");
    };

    let completion_client = client.clone();
    let component_for_completion = component.clone();
    let marker_for_completion = accepted_marker.clone();
    let completion_handle = thread::spawn(move || {
        completion_client.submit(UpgradeOperation::HandoverCompleted(
            signal_version_handover::CompletionReport {
                component: component_for_completion,
                accepted_marker: marker_for_completion,
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves completion exchange");
    let completion_reply = completion_handle
        .join()
        .expect("completion client exits")
        .expect("completion reply received");
    assert_eq!(
        completion_reply,
        UpgradeReply::HandoverFinalized(HandoverFinalization {
            finalized_marker: accepted_marker,
        })
    );

    let mirror_client = client.clone();
    let component_for_mirror = component.clone();
    let mirror_handle = thread::spawn(move || {
        mirror_client.submit(UpgradeOperation::Mirror(
            signal_version_handover::MirrorPayload {
                component: component_for_mirror,
                source_version: ContractVersion::new([2; 32]),
                target_version: ContractVersion::new([9; 32]),
                kind: RecordKind::new("StampedEntry"),
                payload: mirrored_stamped_entry_payload("rejected wrong target"),
            },
        ))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves mirror exchange");
    let mirror_reply = mirror_handle
        .join()
        .expect("mirror client exits")
        .expect("mirror reply received");
    assert_eq!(
        mirror_reply,
        UpgradeReply::HandoverRejected(HandoverRejection {
            component: component.clone(),
            reason: HandoverRejectionReason::SchemaMismatch,
        })
    );

    let marker_client = client.clone();
    let component_for_marker = component.clone();
    let marker_handle = thread::spawn(move || {
        marker_client.submit(UpgradeOperation::AskHandoverMarker(MarkerRequest {
            component: component_for_marker,
        }))
    });
    daemon
        .serve_upgrade_one()
        .expect("daemon serves post-rejection marker exchange");
    let marker_reply = marker_handle
        .join()
        .expect("marker client exits")
        .expect("marker reply received");
    let UpgradeReply::HandoverMarker(marker) = marker_reply else {
        panic!("expected post-rejection marker, got {marker_reply:?}");
    };
    assert_eq!(marker.commit_sequence, 0);
    assert_eq!(marker.write_counter, 0);
    assert_eq!(marker.last_record_identifier, None);

    daemon.shutdown().expect("daemon shuts down");
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
    std::fs::write(&policy_path, "([daemon configured bootstrap policy])")
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
        "(Record (workspace Decision [client socket] [daemon context] Maximum [daemon quote]))"
            .to_string(),
    ])
    .expect("single request argument");

    let client = ClientShape::<Frame, owner_signal_persona_spirit::Frame>::new(
        CommandLineSockets::working_only(fixture.ordinary_socket.as_path().to_path_buf()),
    );
    let reply = client.reply_text(argument).expect("client sends to daemon");

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
        "(Record (workspace Decision [binary file] [daemon context] Maximum [daemon quote]))",
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
