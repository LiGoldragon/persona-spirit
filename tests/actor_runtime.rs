use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{Error, SpiritActorRuntime, StoreLocation, TraceAction, TraceNode};

#[derive(Debug, Clone)]
struct SpiritRuntimeFixture {
    location: StoreLocation,
}

impl SpiritRuntimeFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("persona-spirit-actor-{test_name}-{nanos}.redb"));
        Self {
            location: StoreLocation::new(path),
        }
    }

    async fn runtime(&self) -> SpiritActorRuntime {
        SpiritActorRuntime::start(self.location.clone())
            .await
            .expect("actor runtime starts")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_entry_assertion_runs_through_actor_planes() {
    let fixture = SpiritRuntimeFixture::new("entry-path");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(Entry (workspace Decision \"actor path\" \"actor context\" Maximum \"2026-05-19T18:13:52Z\" \"actor quote\"))")
        .await
        .expect("entry accepted");

    assert_eq!(
        reply.text(),
        "(RecordAccepted ((1 workspace Decision \"actor path\" Maximum)))"
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::SPIRIT_ROOT,
        TraceNode::INGRESS_PHASE,
        TraceNode::NOTA_DECODER,
        TraceNode::DISPATCH_PHASE,
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_WRITER,
        TraceNode::REPLY_TEXT_ENCODER,
        TraceNode::SPIRIT_ROOT,
    ]));
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_WRITER, TraceAction::RecordCommitted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_record_observation_uses_read_plane_without_write_plane() {
    let fixture = SpiritRuntimeFixture::new("read-plane");
    let runtime = fixture.runtime().await;

    runtime
        .submit_text("(Entry (workspace Decision \"summary\" \"context\" Maximum \"2026-05-19T18:13:52Z\" \"quote\"))")
        .await
        .expect("entry accepted");
    let reply = runtime
        .submit_text("(RecordObservation ((None SummaryOnly)))")
        .await
        .expect("records observed");

    assert_eq!(
        reply.text(),
        "(RecordsObserved ([(1 workspace Decision summary Maximum)]))"
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_READER, TraceAction::RecordsRead)
    );
    assert!(!reply.trace().contains(TraceNode::SEMA_WRITER));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_state_observation_uses_state_plane() {
    let fixture = SpiritRuntimeFixture::new("state-plane");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(StateObservation ())")
        .await
        .expect("state observed");

    assert_eq!(reply.text(), "(StateObserved ((Absent None)))");
    assert!(reply.trace().contains(TraceNode::STATE_PLANE));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));
    assert!(!reply.trace().contains(TraceNode::SEMA_READER));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_question_observation_uses_state_plane() {
    let fixture = SpiritRuntimeFixture::new("question-plane");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(QuestionPending ())")
        .await
        .expect("questions observed");

    assert_eq!(reply.text(), "(QuestionsObserved ([]))");
    assert!(reply.trace().contains(TraceNode::STATE_PLANE));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_state_subscription_uses_subscription_plane_after_state_snapshot() {
    let fixture = SpiritRuntimeFixture::new("state-subscription");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(SubscribeState ())")
        .await
        .expect("state subscription opened");

    assert_eq!(
        reply.text(),
        "(StateSubscriptionOpened ((1) (Absent None)))"
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::STATE_PLANE,
        TraceNode::SUBSCRIPTION_PLANE,
        TraceNode::REPLY_TEXT_ENCODER,
    ]));
    assert!(reply.trace().contains_action(
        TraceNode::SUBSCRIPTION_PLANE,
        TraceAction::SubscriptionOpened
    ));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_record_subscription_uses_read_plane_then_subscription_plane() {
    let fixture = SpiritRuntimeFixture::new("record-subscription");
    let runtime = fixture.runtime().await;

    runtime
        .submit_text("(Entry (workspace Decision \"subscription path\" \"context\" Maximum \"2026-05-19T18:13:52Z\" \"quote\"))")
        .await
        .expect("entry accepted");
    let reply = runtime
        .submit_text("(SubscribeRecords (None SummaryOnly))")
        .await
        .expect("record subscription opened");

    assert_eq!(
        reply.text(),
        "(RecordSubscriptionOpened ((1) [(1 workspace Decision \"subscription path\" Maximum)]))"
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_READER,
        TraceNode::SUBSCRIPTION_PLANE,
        TraceNode::REPLY_TEXT_ENCODER,
    ]));
    assert!(reply.trace().contains_action(
        TraceNode::SUBSCRIPTION_PLANE,
        TraceAction::SubscriptionOpened
    ));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_subscription_retractions_use_subscription_plane() {
    let fixture = SpiritRuntimeFixture::new("subscription-retract");
    let runtime = fixture.runtime().await;

    runtime
        .submit_text("(SubscribeState ())")
        .await
        .expect("state subscription opened");
    runtime
        .submit_text("(SubscribeRecords (None SummaryOnly))")
        .await
        .expect("record subscription opened");
    let state_reply = runtime
        .submit_text("(StateSubscriptionRetraction (1))")
        .await
        .expect("state subscription retracted");
    let record_reply = runtime
        .submit_text("(RecordSubscriptionRetraction (1))")
        .await
        .expect("record subscription retracted");

    assert_eq!(state_reply.text(), "(StateSubscriptionRetracted ((1)))");
    assert_eq!(record_reply.text(), "(RecordSubscriptionRetracted ((1)))");
    assert!(state_reply.trace().contains_action(
        TraceNode::SUBSCRIPTION_PLANE,
        TraceAction::SubscriptionRetracted
    ));
    assert!(record_reply.trace().contains_action(
        TraceNode::SUBSCRIPTION_PLANE,
        TraceAction::SubscriptionRetracted
    ));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_unimplemented_statement_uses_reply_shaper_not_store() {
    let fixture = SpiritRuntimeFixture::new("reply-shaper");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(Statement (\"capture this intent\"))")
        .await
        .expect("statement type checked");

    assert_eq!(
        reply.text(),
        "(RequestUnimplemented (Statement NotBuiltYet))"
    );
    assert!(reply.trace().contains(TraceNode::REPLY_SHAPER));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_shutdown_releases_store_for_restart() {
    let fixture = SpiritRuntimeFixture::new("restart");
    let first_runtime = fixture.runtime().await;

    first_runtime
        .submit_text("(Entry (workspace Decision \"restart survives\" \"context\" Maximum \"2026-05-19T18:13:52Z\" \"quote\"))")
        .await
        .expect("entry accepted");
    first_runtime.stop().await.expect("first runtime stops");

    let second_runtime = fixture.runtime().await;
    let reply = second_runtime
        .submit_text("(RecordObservation ((None SummaryOnly)))")
        .await
        .expect("records observed after restart");

    assert_eq!(
        reply.text(),
        "(RecordsObserved ([(1 workspace Decision \"restart survives\" Maximum)]))"
    );

    second_runtime.stop().await.expect("second runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_invalid_text_keeps_typed_decode_error() {
    let fixture = SpiritRuntimeFixture::new("invalid");
    let runtime = fixture.runtime().await;

    let error = runtime
        .submit_text("(UnknownIntent workspace)")
        .await
        .unwrap_err();

    assert!(matches!(error, Error::InvalidSpiritRequest { .. }));

    runtime.stop().await.expect("runtime stops");
}

#[test]
fn persona_spirit_command_line_path_uses_actor_runtime() {
    let source = std::fs::read_to_string(format!("{}/src/runtime.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("runtime source is readable");

    assert!(source.contains("SpiritActorRuntime::submit_text_blocking"));
    assert!(!source.contains("SpiritRuntime::open"));
}

#[test]
fn persona_spirit_uses_kameo_as_only_actor_runtime() {
    let cargo = std::fs::read_to_string(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR")))
        .expect("cargo manifest is readable");

    assert!(cargo.contains("kameo"));
    assert!(!cargo.contains("ractor"));
    assert!(!cargo.contains("actix"));
}

#[test]
fn persona_spirit_actor_types_are_data_bearing() {
    let actors = [
        ("src/actors/decoder.rs", "pub struct NotaDecoder {"),
        ("src/actors/dispatch.rs", "pub struct DispatchPhase {"),
        ("src/actors/ingress.rs", "pub struct IngressPhase {"),
        ("src/actors/reply.rs", "pub struct ReplyShaper {"),
        ("src/actors/reply.rs", "pub struct ReplyTextEncoder {"),
        ("src/actors/root.rs", "pub struct SpiritRoot {"),
        ("src/actors/state.rs", "pub struct StatePlane {"),
        ("src/actors/store.rs", "pub struct RecordStore {"),
        (
            "src/actors/subscription.rs",
            "pub struct SubscriptionPlane {",
        ),
    ];

    for (file, actor_declaration) in actors {
        let source = std::fs::read_to_string(format!("{}/{file}", env!("CARGO_MANIFEST_DIR")))
            .expect("actor source is readable");
        assert!(
            source.contains(actor_declaration),
            "{file} does not contain {actor_declaration}"
        );
    }
}
