use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use meta_signal_spirit::{
    BootstrapPolicy, BootstrapPolicyReloaded, Drain, DrainedAndStopped, Generation, IdentityName,
    IdentityRegistered, IdentityRetired, Operation as MetaOperation, Registration,
    Reply as MetaReply, RequestUnimplemented, Retirement, Start, Started, UnimplementedReason,
};
use nota_next::{NotaEncode, NotaSource};
use persona_spirit::{
    BootstrapPolicySource, Error, SpiritActorRuntime, StoreLocation, TraceAction, TraceNode,
};
use signal_frame::{Caller, ProcessIdentifier, Request};
use signal_spirit::{
    ObserverFilter, Operation as WorkingOperation, RecordIdentifier, Reply as WorkingReply,
    RequestUnimplemented as SpiritRequestUnimplemented,
    UnimplementedReason as SpiritUnimplementedReason,
};

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
        path.push(format!("persona-spirit-actor-{test_name}-{nanos}.sema"));
        Self {
            location: StoreLocation::new(path),
        }
    }

    async fn runtime(&self) -> SpiritActorRuntime {
        SpiritActorRuntime::start(self.location.clone())
            .await
            .expect("actor runtime starts")
    }

    async fn runtime_with_policy_source(
        &self,
        source: BootstrapPolicySource,
    ) -> SpiritActorRuntime {
        SpiritActorRuntime::start_with_bootstrap_policy_source(self.location.clone(), source)
            .await
            .expect("actor runtime starts")
    }
}

fn accepted_identifier(reply: &persona_spirit::RootTextReply) -> RecordIdentifier {
    let reply = NotaSource::new(reply.text())
        .parse::<WorkingReply>()
        .expect("reply decodes");
    let WorkingReply::RecordAccepted(accepted) = reply else {
        panic!("expected RecordAccepted reply, got {reply:?}");
    };
    accepted.identifier()
}

fn identifier_text(identifier: RecordIdentifier) -> String {
    identifier.to_nota()
}

fn assert_short_identifier(identifier: RecordIdentifier) {
    let code_length = identifier.code().len();
    assert!(
        (4..=7).contains(&code_length),
        "identifier should use a four-to-seven-character code: {}",
        identifier.code()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_entry_assertion_runs_through_actor_planes() {
    let fixture = SpiritRuntimeFixture::new("entry-path");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(Record ([workspace] Decision [actor path] Maximum))")
        .await
        .expect("entry accepted");

    let identifier = accepted_identifier(&reply);
    assert_short_identifier(identifier);
    assert_eq!(
        reply.text(),
        format!("(RecordAccepted {})", identifier_text(identifier))
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::SPIRIT_ROOT,
        TraceNode::INGRESS_PHASE,
        TraceNode::NOTA_DECODER,
        TraceNode::DISPATCH_PHASE,
        TraceNode::SIGNAL_EXECUTOR,
        TraceNode::CLOCK_PLANE,
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_WRITER,
        TraceNode::SEMA_OBSERVER,
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
async fn persona_spirit_ordinary_request_path_uses_signal_executor_and_sema_observer() {
    let fixture = SpiritRuntimeFixture::new("signal-executor-path");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_request(WorkingOperation::Record(signal_spirit::Entry {
            topics: signal_spirit::Topics::single(signal_spirit::Topic::new("workspace")),
            kind: signal_spirit::Kind::Decision,
            description: signal_spirit::Description::new("executor path"),
            certainty: signal_spirit::Magnitude::Maximum,
            privacy: signal_spirit::Magnitude::Zero,
        }))
        .await
        .expect("entry accepted");

    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SIGNAL_EXECUTOR, TraceAction::OperationReceived)
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_OBSERVER, TraceAction::ObservationProjected)
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::CLOCK_PLANE, TraceAction::EntryStamped)
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::DISPATCH_PHASE,
        TraceNode::SIGNAL_EXECUTOR,
        TraceNode::CLOCK_PLANE,
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_WRITER,
        TraceNode::SEMA_OBSERVER,
        TraceNode::SPIRIT_ROOT,
    ]));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_frame_request_observes_non_init_caller() {
    let fixture = SpiritRuntimeFixture::new("caller");
    let runtime = fixture.runtime().await;
    let request = Request::from_payload(WorkingOperation::Record(signal_spirit::Entry {
        topics: signal_spirit::Topics::single(signal_spirit::Topic::new("workspace")),
        kind: signal_spirit::Kind::Decision,
        description: signal_spirit::Description::new("caller witness"),
        certainty: signal_spirit::Magnitude::Maximum,
        privacy: signal_spirit::Magnitude::Zero,
    }))
    .with_caller(Some(Caller::new(ProcessIdentifier::new(42), None, None)));

    let reply = runtime
        .submit_frame_request(request)
        .await
        .expect("frame request accepted");

    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SPIRIT_ROOT, TraceAction::CallerObserved)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_record_observation_uses_read_plane_without_write_plane() {
    let fixture = SpiritRuntimeFixture::new("read-plane");
    let runtime = fixture.runtime().await;

    let accepted = runtime
        .submit_text("(Record ([workspace] Decision description Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    let reply = runtime
        .submit_text("(Observe (Records ((Any []) None SummaryOnly)))")
        .await
        .expect("records observed");

    assert_eq!(
        reply.text(),
        format!(
            "(RecordsObserved [({} [workspace] Decision [description] Maximum Zero)])",
            identifier_text(identifier)
        )
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
async fn persona_spirit_record_removal_uses_write_plane() {
    let fixture = SpiritRuntimeFixture::new("record-removal-write-plane");
    let runtime = fixture.runtime().await;

    let accepted = runtime
        .submit_text("(Record ([workspace] Decision description Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    let reply = runtime
        .submit_text(&format!("(Remove {})", identifier_text(identifier)))
        .await
        .expect("entry removed");
    let observed = runtime
        .submit_text("(Observe (Records ((Any []) None SummaryOnly)))")
        .await
        .expect("records observed");

    assert_eq!(
        reply.text(),
        format!("(RecordRemoved {})", identifier_text(identifier))
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_WRITER, TraceAction::RecordRetracted)
    );
    assert_eq!(observed.text(), "(RecordsObserved [])");

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_certainty_change_uses_write_plane() {
    let fixture = SpiritRuntimeFixture::new("certainty-change-write-plane");
    let runtime = fixture.runtime().await;

    let accepted = runtime
        .submit_text("(Record ([workspace] Decision description Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    let reply = runtime
        .submit_text(&format!(
            "(ChangeCertainty ({} Zero))",
            identifier_text(identifier)
        ))
        .await
        .expect("certainty changed");
    let observed = runtime
        .submit_text("(Observe (Records ((Any []) None (Exact Zero) SummaryOnly)))")
        .await
        .expect("records observed");

    assert_eq!(
        reply.text(),
        format!("(CertaintyChanged ({} Zero))", identifier_text(identifier))
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_WRITER, TraceAction::RecordMutated)
    );
    assert_eq!(
        observed.text(),
        format!(
            "(RecordsObserved [({} [workspace] Decision [description] Zero Zero)])",
            identifier_text(identifier)
        )
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_record_change_uses_write_plane_and_preserves_identifier() {
    let fixture = SpiritRuntimeFixture::new("record-change-write-plane");
    let runtime = fixture.runtime().await;

    let accepted = runtime
        .submit_text("(Record ([workspace] Decision [original] Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    let reply = runtime
        .submit_text(&format!(
            "(ChangeRecord ({} ([workspace message] Correction [replacement] High Zero)))",
            identifier_text(identifier)
        ))
        .await
        .expect("record changed");
    let observed = runtime
        .submit_text("(Observe (Records ((Partial [message]) None Any SummaryOnly)))")
        .await
        .expect("records observed");

    assert_eq!(
        reply.text(),
        format!("(RecordMutationApplied {})", identifier_text(identifier))
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_WRITER, TraceAction::RecordMutated)
    );
    assert_eq!(
        observed.text(),
        format!(
            "(RecordsObserved [({} [workspace message] Correction [replacement] High Zero)])",
            identifier_text(identifier)
        )
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_collect_removal_candidates_archives_before_retracting() {
    let fixture = SpiritRuntimeFixture::new("collect-removal-candidates");
    let runtime = fixture.runtime().await;
    let mut archive_path = std::env::temp_dir();
    archive_path.push(format!(
        "persona-spirit-collect-removal-candidates-{}.sema",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));

    let candidate = runtime
        .submit_text("(Record ([workspace] Correction [candidate description] Zero))")
        .await
        .expect("candidate accepted");
    let candidate_identifier = accepted_identifier(&candidate);
    let active_record = runtime
        .submit_text("(Record ([workspace] Decision [active description] High))")
        .await
        .expect("active record accepted");
    let active_identifier = accepted_identifier(&active_record);
    let request = format!(
        "(CollectRemovalCandidates (((Any []) None (Exact Zero) Any (Exact Zero) SummaryOnly) (ArchiveDatabase (Path [{}]))))",
        archive_path.to_string_lossy()
    );
    let reply = runtime
        .submit_text(&request)
        .await
        .expect("candidates collected");
    let candidates = runtime
        .submit_text("(Observe (Records ((Any []) None (Exact Zero) SummaryOnly)))")
        .await
        .expect("candidates observed after collection");
    let active = runtime
        .submit_text("(Observe (Records ((Any []) None (AtLeast Minimum) SummaryOnly)))")
        .await
        .expect("active records observed after collection");

    assert_eq!(
        reply.text(),
        format!(
            "(RemovalCandidatesCollected ([({} [workspace] Correction [candidate description] Zero Zero)] [{}] []))",
            identifier_text(candidate_identifier),
            identifier_text(candidate_identifier)
        )
    );
    assert!(
        fs::metadata(&archive_path)
            .expect("archive database metadata readable")
            .len()
            > 0
    );
    assert_eq!(candidates.text(), "(RecordsObserved [])");
    assert_eq!(
        active.text(),
        format!(
            "(RecordsObserved [({} [workspace] Decision [active description] High Zero)])",
            identifier_text(active_identifier)
        )
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::DISPATCH_PHASE,
        TraceNode::SIGNAL_EXECUTOR,
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_READER,
        TraceNode::SEMA_WRITER,
        TraceNode::SEMA_OBSERVER,
        TraceNode::SPIRIT_ROOT,
    ]));
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_READER, TraceAction::RecordsRead)
    );
    assert!(
        reply
            .trace()
            .contains_action(TraceNode::SEMA_WRITER, TraceAction::RecordRetracted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_topic_catalog_observation_uses_read_plane_without_write_plane() {
    let fixture = SpiritRuntimeFixture::new("topic-catalog");
    let runtime = fixture.runtime().await;

    runtime
        .submit_text("(Record ([spirit] Principle [topic one] Maximum))")
        .await
        .expect("first entry accepted");
    runtime
        .submit_text("(Record ([naming] Correction [topic two] Maximum))")
        .await
        .expect("second entry accepted");
    runtime
        .submit_text("(Record ([spirit] Constraint [topic three] Maximum))")
        .await
        .expect("third entry accepted");

    let reply = runtime
        .submit_text("(Observe Topics)")
        .await
        .expect("topics observed");

    assert_eq!(reply.text(), "(TopicsObserved [([naming] 1) ([spirit] 2)])");
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
        .submit_text("(Observe State)")
        .await
        .expect("state observed");

    assert_eq!(reply.text(), "(StateObserved (Absent None))");
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
        .submit_text("(Observe Questions)")
        .await
        .expect("questions observed");

    assert_eq!(reply.text(), "(QuestionsObserved [])");
    assert!(reply.trace().contains(TraceNode::STATE_PLANE));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_unimplemented_observer_request_uses_reply_shaper_not_store() {
    let fixture = SpiritRuntimeFixture::new("unimplemented-observer");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_request(WorkingOperation::Tap(ObserverFilter::All))
        .await
        .expect("observer tap is handled as an unimplemented request");

    assert_eq!(
        reply.reply(),
        &WorkingReply::RequestUnimplemented(SpiritRequestUnimplemented {
            reason: SpiritUnimplementedReason::NotBuiltYet,
        })
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::DISPATCH_PHASE,
        TraceNode::REPLY_SHAPER,
        TraceNode::SPIRIT_ROOT,
    ]));
    assert!(!reply.trace().contains(TraceNode::RECORD_STORE));
    assert!(!reply.trace().contains(TraceNode::SEMA_WRITER));
    assert!(!reply.trace().contains(TraceNode::SEMA_READER));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_state_subscription_uses_subscription_plane_after_state_snapshot() {
    let fixture = SpiritRuntimeFixture::new("state-subscription");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(Watch State)")
        .await
        .expect("state subscription opened");

    assert_eq!(
        reply.text(),
        "(SubscriptionOpened ((State (1)) (State (Absent None))))"
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

    let accepted = runtime
        .submit_text("(Record ([workspace] Decision [subscription path] Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    let reply = runtime
        .submit_text("(Watch (Records (None SummaryOnly)))")
        .await
        .expect("record subscription opened");

    assert_eq!(
        reply.text(),
        format!(
            "(SubscriptionOpened ((Records (1)) (Records [({} [workspace] Decision [subscription path] Maximum Zero)])))",
            identifier_text(identifier)
        )
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
        .submit_text("(Watch State)")
        .await
        .expect("state subscription opened");
    runtime
        .submit_text("(Watch (Records (None SummaryOnly)))")
        .await
        .expect("record subscription opened");
    let state_reply = runtime
        .submit_text("(Unwatch (State (1)))")
        .await
        .expect("state subscription retracted");
    let record_reply = runtime
        .submit_text("(Unwatch (Records (1)))")
        .await
        .expect("record subscription retracted");

    assert_eq!(state_reply.text(), "(SubscriptionRetracted ((State (1))))");
    assert_eq!(
        record_reply.text(),
        "(SubscriptionRetracted ((Records (1))))"
    );
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
async fn persona_spirit_meta_lifecycle_orders_use_meta_plane() {
    let fixture = SpiritRuntimeFixture::new("meta-lifecycle");
    let runtime = fixture.runtime().await;

    let started = runtime
        .submit_meta_request(MetaOperation::Start(Start {
            generation: Generation::new(7),
        }))
        .await
        .expect("meta start accepted");
    let stopped = runtime
        .submit_meta_request(MetaOperation::Drain(Drain {}))
        .await
        .expect("meta drain accepted");

    assert_eq!(
        started.reply(),
        &MetaReply::Started(Started {
            generation: Generation::new(7),
        })
    );
    assert_eq!(
        stopped.reply(),
        &MetaReply::DrainedAndStopped(DrainedAndStopped {})
    );
    assert!(started.trace().contains_ordered(&[
        TraceNode::SPIRIT_ROOT,
        TraceNode::META_PLANE,
        TraceNode::SPIRIT_ROOT,
    ]));
    assert!(!started.trace().contains(TraceNode::DISPATCH_PHASE));
    assert!(!started.trace().contains(TraceNode::RECORD_STORE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_meta_identity_orders_use_meta_plane() {
    let fixture = SpiritRuntimeFixture::new("meta-identity");
    let runtime = fixture.runtime().await;

    let registered = runtime
        .submit_meta_request(MetaOperation::Register(Registration {
            name: IdentityName::new("author"),
        }))
        .await
        .expect("identity registered");
    let retired = runtime
        .submit_meta_request(MetaOperation::Retire(Retirement {
            name: IdentityName::new("author"),
        }))
        .await
        .expect("identity retired");

    assert_eq!(
        registered.reply(),
        &MetaReply::IdentityRegistered(IdentityRegistered {
            name: IdentityName::new("author"),
        })
    );
    assert_eq!(
        retired.reply(),
        &MetaReply::IdentityRetired(IdentityRetired {
            name: IdentityName::new("author"),
        })
    );
    assert!(registered.trace().contains(TraceNode::META_PLANE));
    assert!(retired.trace().contains(TraceNode::META_PLANE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_bootstrap_policy_reload_uses_policy_plane() {
    let fixture = SpiritRuntimeFixture::new("meta-policy");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_meta_request(MetaOperation::Reload(BootstrapPolicy {}))
        .await
        .expect("policy reload type checked");

    assert_eq!(
        reply.reply(),
        &MetaReply::BootstrapPolicyReloaded(BootstrapPolicyReloaded {})
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::META_PLANE,
        TraceNode::POLICY_PLANE,
        TraceNode::META_PLANE,
    ]));
    assert!(!reply.trace().contains(TraceNode::DISPATCH_PHASE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_bootstrap_policy_reload_reports_missing_policy_source() {
    let fixture = SpiritRuntimeFixture::new("missing-policy");
    let mut missing = std::env::temp_dir();
    missing.push("persona-spirit-missing-bootstrap-policy.nota");
    let runtime = fixture
        .runtime_with_policy_source(BootstrapPolicySource::path(missing))
        .await;

    let reply = runtime
        .submit_meta_request(MetaOperation::Reload(BootstrapPolicy {}))
        .await
        .expect("policy reload type checked");

    assert_eq!(
        reply.reply(),
        &MetaReply::RequestUnimplemented(RequestUnimplemented {
            reason: UnimplementedReason::DependencyNotReady,
        })
    );
    assert!(reply.trace().contains(TraceNode::POLICY_PLANE));
    assert!(!reply.trace().contains(TraceNode::DISPATCH_PHASE));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_state_statement_uses_classifier_before_store() {
    let fixture = SpiritRuntimeFixture::new("classifier");
    let runtime = fixture.runtime().await;

    let reply = runtime
        .submit_text("(State ([capture this intent]))")
        .await
        .expect("statement classified");

    let identifier = accepted_identifier(&reply);
    assert_short_identifier(identifier);
    assert_eq!(
        reply.text(),
        format!("(RecordAccepted {})", identifier_text(identifier))
    );
    assert!(reply.trace().contains_ordered(&[
        TraceNode::DISPATCH_PHASE,
        TraceNode::CLASSIFIER_PLANE,
        TraceNode::CLOCK_PLANE,
        TraceNode::RECORD_STORE,
        TraceNode::SEMA_WRITER,
        TraceNode::REPLY_TEXT_ENCODER,
    ]));
    assert!(reply.trace().contains_action(
        TraceNode::CLASSIFIER_PLANE,
        TraceAction::StatementClassified
    ));

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persona_spirit_shutdown_releases_store_for_restart() {
    let fixture = SpiritRuntimeFixture::new("restart");
    let first_runtime = fixture.runtime().await;

    let accepted = first_runtime
        .submit_text("(Record ([workspace] Decision [restart survives] Maximum))")
        .await
        .expect("entry accepted");
    let identifier = accepted_identifier(&accepted);
    first_runtime.stop().await.expect("first runtime stops");

    let second_runtime = fixture.runtime().await;
    let reply = second_runtime
        .submit_text("(Observe (Records ((Any []) None SummaryOnly)))")
        .await
        .expect("records observed after restart");

    assert_eq!(
        reply.text(),
        format!(
            "(RecordsObserved [({} [workspace] Decision [restart survives] Maximum Zero)])",
            identifier_text(identifier)
        )
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
fn persona_spirit_command_line_path_does_not_use_actor_runtime_directly() {
    let source =
        std::fs::read_to_string(format!("{}/src/bin/spirit.rs", env!("CARGO_MANIFEST_DIR")))
            .expect("spirit binary source is readable");

    assert!(source.contains("persona_spirit::CommandLine::run_from_environment(\"spirit\")"));
    assert!(!source.contains("SpiritActorRuntime"));
    assert!(!source.contains("StoreLocation"));
}

#[test]
fn persona_spirit_public_surface_uses_side_modules_instead_of_spirit_prefixes() {
    let source = std::fs::read_to_string(format!("{}/src/lib.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("lib source is readable");

    assert!(source.contains("pub mod ordinary"));
    assert!(source.contains("pub mod meta"));
    assert!(!source.contains("SpiritClient"));
    assert!(!source.contains("SpiritFrameCodec"));
    assert!(!source.contains("SpiritSignalClient"));
    assert!(!source.contains("MetaSignalClient"));
    assert!(!source.contains("SpiritCommandLine"));
    assert!(!source.contains("SpiritRequestText"));
    assert!(!source.contains("SpiritReplyText"));
    assert!(!source.contains("MetaSpiritRequestText"));
    assert!(!source.contains("MetaSpiritReplyText"));
}

#[test]
fn persona_spirit_dispatch_path_depends_on_signal_executor() {
    let manifest = std::fs::read_to_string(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR")))
        .expect("cargo manifest is readable");
    let source = std::fs::read_to_string(format!(
        "{}/src/actors/dispatch.rs",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("dispatch source is readable");

    assert!(manifest.contains("signal-executor"));
    assert!(source.contains("signal_executor::"));
    assert!(source.contains("Executor::new"));
    assert!(source.contains(".execute(request).await"));
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
        ("src/actors/clock.rs", "pub struct ClockPlane {"),
        ("src/actors/classifier.rs", "pub struct ClassifierPlane {"),
        ("src/actors/dispatch.rs", "pub struct DispatchPhase {"),
        ("src/actors/ingress.rs", "pub struct IngressPhase {"),
        ("src/actors/meta.rs", "pub struct MetaPlane {"),
        ("src/actors/policy.rs", "pub struct PolicyPlane {"),
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
