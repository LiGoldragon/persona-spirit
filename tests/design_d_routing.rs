use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use persona::engine::SocketMode as PersonaSocketMode;
use persona::transport::{ComponentHandoffEndpoint, ComponentHandoffRouter};
use persona::upgrade::Version;
use persona_spirit::{
    DaemonConfiguration, DaemonRuntime, ServedExchange, SocketMode, SocketPath, StorePath,
};

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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "persona-spirit-design-d-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("route fixture root created");
        Self {
            public_socket: root.join("persona").join("spirit.sock"),
            control_socket: root.join("persona").join("control").join("spirit.sock"),
            ordinary_socket: socket_path(&root, "daemon-private-spirit.sock"),
            owner_socket: socket_path(&root, "daemon-private-spirit-owner.sock"),
            upgrade_socket: socket_path(&root, "daemon-private-spirit-upgrade.sock"),
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

#[test]
fn persona_spirit_cli_reaches_daemon_through_persona_handoff_router() {
    let fixture = RouteFixture::new("spirit-cli");
    let version = Version::new("v0.1.0");
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
        "(Record (workspace Decision [design d route] [through Persona public socket] Maximum [persona hands off descriptor]))",
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
        "(Observe (Records (None None SummaryOnly)))",
    );
    runtime
        .block_on(router.handoff_one(&version))
        .expect("Persona hands observe client descriptor to Spirit");
    assert_spirit_output(
        observe_output.join().expect("observe client exits"),
        "(RecordsObserved ([(1 workspace Decision [design d route] Maximum)]))",
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
