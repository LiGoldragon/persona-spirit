use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use owner_signal_version_handover::{
    AttemptHandover, ForceFlip, ForceReason, Operation as OwnerOperation, Reply as OwnerReply,
    SocketPath as OwnerSocketPath, Version as OwnerVersion, VersionEndpoint, VersionLabel,
};
use persona::engine::SocketMode as PersonaSocketMode;
use persona::manager::{EngineManager, HandleOwnerVersionHandover};
use persona::manager_store::{ManagerStore, ManagerStoreLocation};
use persona::transport::{
    ComponentHandoffEndpoint, ComponentHandoffRouter, ManagerStoreActiveVersionReader,
};
use persona::unit::ManualUnitController;
use persona::upgrade::Version as PersonaVersion;
use persona_spirit::{
    DaemonConfiguration, DaemonRuntime, ServedExchange, SocketMode, SocketPath, StorePath, ordinary,
};
use signal_frame::{Reply, SubReply};
use signal_persona_origin::EngineIdentifier;
use signal_persona_spirit::{
    CertaintySelection, Observation, Operation as SpiritOperation, RecordQuery,
    Reply as SpiritReply, TopicSelection,
};
use version_projection::{ComponentName, ContractVersion};

struct RouteFixture {
    root: PathBuf,
    public_socket: PathBuf,
    control_socket: PathBuf,
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    upgrade_socket: SocketPath,
    store: StorePath,
}

impl RouteFixture {
    fn new(name: &str) -> Self {
        let root = short_root(name);
        std::fs::create_dir_all(&root).expect("route fixture root created");
        Self {
            public_socket: root.join("p.sock"),
            control_socket: root.join("c.sock"),
            ordinary_socket: socket_path(&root, "o.sock"),
            owner_socket: socket_path(&root, "w.sock"),
            upgrade_socket: socket_path(&root, "u.sock"),
            store: StorePath::new(
                root.join("persona-spirit.redb")
                    .to_string_lossy()
                    .into_owned(),
            ),
            root,
        }
    }

    fn endpoint(&self) -> ComponentHandoffEndpoint {
        ComponentHandoffEndpoint::for_component_name(
            "persona-spirit",
            &self.public_socket,
            &self.control_socket,
            PersonaSocketMode::internal_component(),
        )
    }

    fn configuration(&self) -> DaemonConfiguration {
        DaemonConfiguration::new(
            self.ordinary_socket.clone(),
            self.owner_socket.clone(),
            self.upgrade_socket.clone(),
            self.store.clone(),
            SocketMode::from_octal(0o600),
        )
        .with_handoff_control_socket_path(SocketPath::new(
            self.control_socket.to_string_lossy().into_owned(),
        ))
    }
}

impl Drop for RouteFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn short_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("psd-{name}-{}-{nanos:x}", std::process::id()))
}

fn socket_path(root: &Path, file_name: &str) -> SocketPath {
    SocketPath::new(root.join(file_name).to_string_lossy().into_owned())
}

fn spawn_spirit(public_socket: PathBuf, request: &'static str) -> thread::JoinHandle<Output> {
    thread::spawn(move || {
        Command::new(env!("CARGO_BIN_EXE_spirit"))
            .env("PERSONA_SPIRIT_SOCKET", public_socket)
            .env_remove("PERSONA_SPIRIT_OWNER_SOCKET")
            .arg(request)
            .output()
            .expect("spirit binary runs")
    })
}

fn assert_spirit_output(output: Output, expected_stdout: &str) {
    assert!(
        output.status.success(),
        "spirit stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        expected_stdout
    );
}

struct SpiritInstance {
    ordinary_socket: SocketPath,
    owner_socket: SocketPath,
    upgrade_socket: SocketPath,
    store: StorePath,
}

impl SpiritInstance {
    fn new(root: &Path, name: &str) -> Self {
        let root = root.join(name);
        std::fs::create_dir_all(&root).expect("spirit instance root created");
        Self {
            ordinary_socket: socket_path(&root, "spirit.sock"),
            owner_socket: socket_path(&root, "owner.sock"),
            upgrade_socket: socket_path(&root, "upgrade.sock"),
            store: StorePath::new(root.join("spirit.redb").to_string_lossy().into_owned()),
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

    fn configuration_with_handoff(&self, control_socket: &Path) -> DaemonConfiguration {
        self.configuration()
            .with_handoff_control_socket_path(SocketPath::new(
                control_socket.to_string_lossy().into_owned(),
            ))
    }

    fn upgrade_socket_path(&self) -> &Path {
        self.upgrade_socket.as_path()
    }

    fn store_path(&self) -> &Path {
        self.store.as_path()
    }

    fn copy_store_from(&self, source: &Self) {
        std::fs::copy(source.store_path(), self.store_path()).expect("store copy succeeds");
    }
}

fn observe_records() -> SpiritOperation {
    SpiritOperation::Observe(Observation::Records(RecordQuery {
        topic_selection: TopicSelection::any(),
        kind: None,
        certainty_selection: CertaintySelection::Any,
        recorded_time_selection: signal_persona_spirit::RecordedTimeSelection::Any,
        mode: signal_persona_spirit::ObservationMode::SummaryOnly,
    }))
}

fn owner_version(label: &str, byte: u8) -> OwnerVersion {
    OwnerVersion::new(VersionLabel::new(label), ContractVersion::new([byte; 32]))
}

fn owner_endpoint(label: &str, byte: u8, upgrade_socket_path: &Path) -> VersionEndpoint {
    VersionEndpoint {
        version: owner_version(label, byte),
        owner_socket_path: OwnerSocketPath::new(format!("/unused/{label}/owner.sock")),
        upgrade_socket_path: OwnerSocketPath::new(
            upgrade_socket_path.to_string_lossy().into_owned(),
        ),
    }
}

fn attempt_handover(current: &SpiritInstance, next: &SpiritInstance) -> AttemptHandover {
    AttemptHandover {
        component: ComponentName::new("persona-spirit"),
        current: owner_endpoint("v0.1.0", 1, current.upgrade_socket_path()),
        next: owner_endpoint("v0.1.1", 2, next.upgrade_socket_path()),
    }
}

fn initial_force_flip() -> ForceFlip {
    ForceFlip {
        component: ComponentName::new("persona-spirit"),
        current_version: owner_version("none", 0),
        target_version: owner_version("v0.1.0", 1),
        reason: ForceReason::OperatorOverride,
    }
}

fn assert_records_reply(reply: Reply<SpiritReply>, expected_description: &str) {
    let Reply::Accepted { per_operation, .. } = reply else {
        panic!("expected accepted reply, got {reply:?}");
    };
    let mut operations = per_operation.into_vec();
    assert_eq!(operations.len(), 1);
    let operation = operations.remove(0);
    let SubReply::Ok(SpiritReply::RecordsObserved(records)) = operation else {
        panic!("expected RecordsObserved reply, got {operation:?}");
    };
    assert_eq!(records.records().len(), 1);
    assert_eq!(
        records.records()[0].description.as_str(),
        expected_description
    );
}

#[test]
fn persona_spirit_cli_reaches_daemon_through_persona_handoff_router() {
    let fixture = RouteFixture::new("spirit-cli");
    let version = PersonaVersion::new("v0.1.0");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime builds");
    let mut router = runtime
        .block_on(async { ComponentHandoffRouter::bind(fixture.endpoint()) })
        .expect("Persona router binds");
    let mut daemon = DaemonRuntime::from_configuration(fixture.configuration())
        .bind()
        .expect("Spirit daemon binds and connects to Persona control socket");
    runtime
        .block_on(router.accept_receiver_for_version(version.clone()))
        .expect("Spirit daemon receiver registers for active version");

    let daemon_thread = thread::spawn(move || -> persona_spirit::Result<Vec<ServedExchange>> {
        let first = daemon.serve_handoff_one()?;
        let second = daemon.serve_handoff_one()?;
        daemon.shutdown()?;
        Ok(vec![first, second])
    });

    let record_output = spawn_spirit(
        fixture.public_socket.clone(),
        "(Record ([workspace] Decision [design d route] Maximum))",
    );
    runtime
        .block_on(router.handoff_one(&version))
        .expect("Persona hands record client descriptor to Spirit");
    assert_spirit_output(
        record_output.join().expect("record client exits"),
        "(RecordAccepted 1)",
    );

    let observe_output = spawn_spirit(
        fixture.public_socket.clone(),
        "(Observe (Records ((Any []) None SummaryOnly)))",
    );
    runtime
        .block_on(router.handoff_one(&version))
        .expect("Persona hands observe client descriptor to Spirit");
    assert_spirit_output(
        observe_output.join().expect("observe client exits"),
        "(RecordsObserved [(1 [workspace] Decision [design d route] Maximum)])",
    );

    let served = daemon_thread
        .join()
        .expect("daemon thread exits")
        .expect("daemon served handed-off clients");
    assert_eq!(served.len(), 2);
    assert!(
        !fixture.ordinary_socket.as_path().exists(),
        "Spirit daemon never needed the public client on its private ordinary socket"
    );
}

#[test]
fn persona_handoff_router_routes_new_connections_after_selector_flip_and_old_connections_drain() {
    let root = short_root("flip");
    std::fs::create_dir_all(&root).expect("selector flip root created");
    let public_socket = root.join("p.sock");
    let control_socket = root.join("c.sock");
    let current = SpiritInstance::new(&root, "current");
    let next = SpiritInstance::new(&root, "next");

    let seed_daemon = DaemonRuntime::from_configuration(current.configuration())
        .bind()
        .expect("seed Spirit daemon binds");
    let seed_thread = thread::spawn(move || seed_daemon.serve_count(1));
    let seed_output = spawn_spirit(
        current.ordinary_socket.as_path().to_path_buf(),
        "(Record ([workspace] Decision [selector seed] Maximum))",
    );
    assert_spirit_output(
        seed_output.join().expect("seed client exits"),
        "(RecordAccepted 1)",
    );
    seed_thread
        .join()
        .expect("seed daemon thread joins")
        .expect("seed daemon exits");
    next.copy_store_from(&current);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime builds");
    let endpoint = ComponentHandoffEndpoint::for_component_name(
        "persona-spirit",
        &public_socket,
        &control_socket,
        PersonaSocketMode::internal_component(),
    );
    let mut router = runtime
        .block_on(async { ComponentHandoffRouter::bind(endpoint) })
        .expect("Persona handoff router binds");

    let current_daemon =
        DaemonRuntime::from_configuration(current.configuration_with_handoff(&control_socket))
            .bind()
            .expect("current Spirit daemon binds and connects to Persona control socket");
    runtime
        .block_on(router.accept_receiver_for_version(PersonaVersion::new("v0.1.0")))
        .expect("current Spirit receiver registers");
    let next_daemon =
        DaemonRuntime::from_configuration(next.configuration_with_handoff(&control_socket))
            .bind()
            .expect("next Spirit daemon binds and connects to Persona control socket");
    runtime
        .block_on(router.accept_receiver_for_version(PersonaVersion::new("v0.1.1")))
        .expect("next Spirit receiver registers");

    let current_thread =
        thread::spawn(move || current_daemon.serve_handoff_and_upgrade_counts(2, 3));
    let next_thread = thread::spawn(move || next_daemon.serve_handoff_and_upgrade_counts(1, 1));

    let engine = EngineIdentifier::new("selector-flip-engine");
    let store = runtime
        .block_on(async {
            ManagerStore::start(ManagerStoreLocation::new(root.join("manager.redb")))
        })
        .expect("manager store starts");
    let manager = runtime
        .block_on(EngineManager::start_with_store_and_unit_controller(
            engine.clone(),
            store.clone(),
            Arc::new(ManualUnitController),
        ))
        .expect("engine manager starts");
    let active_version_reader = ManagerStoreActiveVersionReader::for_component_name(
        engine.clone(),
        "persona-spirit",
        store.clone(),
    );

    let initial_reply = runtime
        .block_on(
            manager
                .ask(HandleOwnerVersionHandover::new(OwnerOperation::ForceFlip(
                    initial_force_flip(),
                )))
                .send(),
        )
        .expect("initial force flip succeeds");
    let OwnerReply::FlipForced(initial_flip) = initial_reply else {
        panic!("expected initial FlipForced reply, got {initial_reply:?}");
    };
    assert_eq!(initial_flip.active_version.label.as_str(), "v0.1.0");

    let steady_output = spawn_spirit(
        public_socket.clone(),
        "(Observe (Records ((Any []) None SummaryOnly)))",
    );
    runtime
        .block_on(router.handoff_one_from_manager_store(&active_version_reader))
        .expect("Persona routes steady-state client to current version");
    assert_spirit_output(
        steady_output.join().expect("steady-state client exits"),
        "(RecordsObserved [(1 [workspace] Decision [selector seed] Maximum)])",
    );

    let mut old_stream =
        UnixStream::connect(&public_socket).expect("old client connects before selector flip");
    runtime
        .block_on(router.handoff_one_from_manager_store(&active_version_reader))
        .expect("Persona hands old client descriptor to current version");

    let handover_reply = runtime
        .block_on(
            manager
                .ask(HandleOwnerVersionHandover::new(
                    OwnerOperation::AttemptHandover(attempt_handover(&current, &next)),
                ))
                .send(),
        )
        .expect("owner attempt handover returns a typed reply");
    let OwnerReply::HandoverSucceeded(success) = handover_reply else {
        panic!("expected HandoverSucceeded reply, got {handover_reply:?}");
    };
    assert_eq!(success.active_version.label.as_str(), "v0.1.1");

    let codec = ordinary::FrameCodec::default();
    let request = codec.request_frame(observe_records());
    codec
        .write_frame(&mut old_stream, &request)
        .expect("old client writes after selector flip");
    let old_reply = codec
        .read_frame(&mut old_stream)
        .and_then(|frame| codec.reply_from_frame(frame))
        .expect("old client receives reply after selector flip");
    assert_records_reply(old_reply, "selector seed");

    let new_output = spawn_spirit(
        public_socket.clone(),
        "(Observe (Records ((Any []) None SummaryOnly)))",
    );
    runtime
        .block_on(router.handoff_one_from_manager_store(&active_version_reader))
        .expect("Persona routes new client to next version after selector flip");
    assert_spirit_output(
        new_output.join().expect("new client exits"),
        "(RecordsObserved [(1 [workspace] Decision [selector seed] Maximum)])",
    );

    let (current_handoffs, current_upgrades) = current_thread
        .join()
        .expect("current daemon thread joins")
        .expect("current daemon serves handoff and upgrade traffic");
    assert_eq!(current_handoffs.len(), 2);
    assert_eq!(current_upgrades.len(), 3);
    let (next_handoffs, next_upgrades) = next_thread
        .join()
        .expect("next daemon thread joins")
        .expect("next daemon serves handoff and upgrade traffic");
    assert_eq!(next_handoffs.len(), 1);
    assert_eq!(next_upgrades.len(), 1);

    runtime
        .block_on(EngineManager::stop(manager))
        .expect("engine manager stops");
    runtime
        .block_on(ManagerStore::close_and_stop(store))
        .expect("manager store closes");
    std::fs::remove_dir_all(root).expect("selector flip fixture removed");
}
