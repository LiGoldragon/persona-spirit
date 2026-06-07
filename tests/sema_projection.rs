use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{Command, Effect, SpiritActorRuntime, StoreLocation};
use signal_frame::SubscriptionTokenInner;
use signal_persona_spirit::{
    CertaintyChange, CertaintySelection, Description, Entry, Kind, Observation, ObservationMode,
    ObserverFilter, ObserverSubscriptionToken, Operation as WorkingOperation, PublicRecordQuery,
    RecordChange, RecordIdentifier, RecordIdentifierQuery, RecordIdentifierSelection,
    RecordedTimeSelection, RemovalCandidateCollection, Reply as WorkingReply,
    StateSubscriptionToken, Statement, StatementText, Subscription, SubscriptionToken, Topic,
    TopicSelection, Topics,
};
use signal_sema::{Magnitude, SemaObservation, SemaOperation, SemaOutcome};

#[derive(Debug, Clone)]
struct RuntimeFixture {
    location: StoreLocation,
}

impl RuntimeFixture {
    fn new(test_name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "persona-spirit-sema-projection-{test_name}-{nanos}.sema"
        ));
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

fn entry(description: &str) -> Entry {
    Entry {
        topics: Topics::single(Topic::new("workspace")),
        kind: Kind::Decision,
        description: Description::new(description),
        certainty: Magnitude::Maximum,
        privacy: Magnitude::Zero,
    }
}

fn observation_for(request: WorkingOperation, reply: WorkingReply) -> SemaObservation {
    let command = Command::from_request(request).expect("ordinary request projects to command");
    let effect = Effect::from_reply(reply);
    effect.sema_observation_for(&command)
}

fn assert_runtime_projection_trace(trace: &persona_spirit::ActorTrace) {
    assert!(trace.contains_action(
        persona_spirit::TraceNode::SIGNAL_EXECUTOR,
        persona_spirit::TraceAction::OperationReceived
    ));
    assert!(trace.contains_action(
        persona_spirit::TraceNode::SEMA_OBSERVER,
        persona_spirit::TraceAction::ObservationProjected
    ));
}

fn accepted_identifier(reply: WorkingReply) -> RecordIdentifier {
    let WorkingReply::RecordAccepted(accepted) = reply else {
        panic!("expected RecordAccepted reply, got {reply:?}");
    };
    accepted.identifier()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_record_assertion_projects_to_asserted_observation() {
    let fixture = RuntimeFixture::new("record-assertion");
    let runtime = fixture.runtime().await;
    let request = WorkingOperation::Record(entry("asserted projection"));
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("record accepted");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Assert, SemaOutcome::Asserted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_statement_classification_projects_to_asserted_observation() {
    let fixture = RuntimeFixture::new("statement");
    let runtime = fixture.runtime().await;
    let request = WorkingOperation::State(Statement {
        text: StatementText::new("capture this statement"),
    });
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("statement classified");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Assert, SemaOutcome::Asserted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_record_query_projects_to_matched_observation() {
    let fixture = RuntimeFixture::new("record-query");
    let runtime = fixture.runtime().await;
    runtime
        .submit_request(WorkingOperation::Record(entry("matched projection")))
        .await
        .expect("record accepted");
    let request = WorkingOperation::Observe(Observation::Records(PublicRecordQuery {
        topic_selection: TopicSelection::any(),
        kind: None,
        certainty_selection: CertaintySelection::Any,
        recorded_time_selection: RecordedTimeSelection::Any,
        mode: ObservationMode::SummaryOnly,
    }));
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("records observed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Match, SemaOutcome::Matched)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_record_identifier_query_projects_to_matched_observation() {
    let fixture = RuntimeFixture::new("record-identifier-query");
    let runtime = fixture.runtime().await;
    let accepted = runtime
        .submit_request(WorkingOperation::Record(entry("matched projection")))
        .await
        .expect("record accepted");
    let identifier = accepted_identifier(accepted.into_reply());
    let request =
        WorkingOperation::Observe(Observation::RecordIdentifiers(RecordIdentifierQuery::new(
            RecordIdentifierSelection::Exact(identifier),
            ObservationMode::SummaryOnly,
        )));
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("record identifier observed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Match, SemaOutcome::Matched)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_record_removal_projects_to_retracted_observation() {
    let fixture = RuntimeFixture::new("record-removal");
    let runtime = fixture.runtime().await;
    let accepted = runtime
        .submit_request(WorkingOperation::Record(entry("removed projection")))
        .await
        .expect("record accepted");
    let identifier = accepted_identifier(accepted.into_reply());
    let request = WorkingOperation::Remove(identifier);
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("record removed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Retract, SemaOutcome::Retracted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_certainty_change_projects_to_mutated_observation() {
    let fixture = RuntimeFixture::new("certainty-change");
    let runtime = fixture.runtime().await;
    let accepted = runtime
        .submit_request(WorkingOperation::Record(entry("changed projection")))
        .await
        .expect("record accepted");
    let identifier = accepted_identifier(accepted.into_reply());
    let request = WorkingOperation::ChangeCertainty(CertaintyChange {
        identifier,
        certainty: Magnitude::Zero,
    });
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("certainty changed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Mutate, SemaOutcome::Mutated)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_record_change_projects_to_mutated_observation() {
    let fixture = RuntimeFixture::new("record-change");
    let runtime = fixture.runtime().await;
    let accepted = runtime
        .submit_request(WorkingOperation::Record(entry("record change projection")))
        .await
        .expect("record accepted");
    let identifier = accepted_identifier(accepted.into_reply());
    let request = WorkingOperation::ChangeRecord(RecordChange {
        record_identifier: identifier,
        entry: Entry {
            topics: Topics::single(Topic::new("message")),
            kind: Kind::Correction,
            description: Description::new("record changed projection"),
            certainty: Magnitude::High,
            privacy: Magnitude::Zero,
        },
    });
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("record changed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Mutate, SemaOutcome::Mutated)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_collect_removal_candidates_projects_to_retracted_observation() {
    let fixture = RuntimeFixture::new("collect-removal-candidates");
    let runtime = fixture.runtime().await;
    let mut candidate = entry("collected projection");
    candidate.certainty = Magnitude::Zero;
    runtime
        .submit_request(WorkingOperation::Record(candidate))
        .await
        .expect("candidate accepted");
    let request = WorkingOperation::CollectRemovalCandidates(
        RemovalCandidateCollection::default_archive_database(),
    );
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("candidates collected");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Retract, SemaOutcome::Retracted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_topic_catalog_query_projects_to_matched_observation() {
    let fixture = RuntimeFixture::new("topic-query");
    let runtime = fixture.runtime().await;
    runtime
        .submit_request(WorkingOperation::Record(entry("matched projection")))
        .await
        .expect("record accepted");
    let request = WorkingOperation::Observe(Observation::Topics);
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("topics observed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Match, SemaOutcome::Matched)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_state_query_projects_to_matched_observation() {
    let fixture = RuntimeFixture::new("state-query");
    let runtime = fixture.runtime().await;
    let request = WorkingOperation::Observe(Observation::State);
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("state observed");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Match, SemaOutcome::Matched)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_state_subscription_projects_to_subscribed_observation() {
    let fixture = RuntimeFixture::new("state-subscription");
    let runtime = fixture.runtime().await;
    let request = WorkingOperation::Watch(Subscription::State);
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("subscription opened");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Subscribe, SemaOutcome::Subscribed)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_state_subscription_retraction_projects_to_retracted_observation() {
    let fixture = RuntimeFixture::new("state-retraction");
    let runtime = fixture.runtime().await;
    runtime
        .submit_request(WorkingOperation::Watch(Subscription::State))
        .await
        .expect("subscription opened");
    let request = WorkingOperation::Unwatch(SubscriptionToken::State(StateSubscriptionToken {
        identifier: 1,
    }));
    let runtime_reply = runtime
        .submit_request(request.clone())
        .await
        .expect("subscription retracted");
    assert_runtime_projection_trace(runtime_reply.trace());
    let reply = runtime_reply.into_reply();

    assert_eq!(
        observation_for(request, reply),
        SemaObservation::new(SemaOperation::Retract, SemaOutcome::Retracted)
    );

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spirit_unimplemented_observer_operations_project_as_explicit_no_change_commands() {
    let fixture = RuntimeFixture::new("observer-no-change");
    let runtime = fixture.runtime().await;

    let tap_request = WorkingOperation::Tap(ObserverFilter::All);
    let tap_runtime_reply = runtime
        .submit_request(tap_request.clone())
        .await
        .expect("tap handled as unimplemented operation");
    assert_runtime_projection_trace(tap_runtime_reply.trace());
    let tap_reply = tap_runtime_reply.into_reply();
    assert_eq!(
        observation_for(tap_request, tap_reply),
        SemaObservation::new(SemaOperation::Subscribe, SemaOutcome::NoChange)
    );

    let untap_request = WorkingOperation::Untap(ObserverSubscriptionToken::new(
        SubscriptionTokenInner::new(1),
    ));
    let untap_runtime_reply = runtime
        .submit_request(untap_request.clone())
        .await
        .expect("untap handled as unimplemented operation");
    assert_runtime_projection_trace(untap_runtime_reply.trace());
    let untap_reply = untap_runtime_reply.into_reply();
    assert_eq!(
        observation_for(untap_request, untap_reply),
        SemaObservation::new(SemaOperation::Retract, SemaOutcome::NoChange)
    );

    runtime.stop().await.expect("runtime stops");
}
