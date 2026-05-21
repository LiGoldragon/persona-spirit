use std::time::{SystemTime, UNIX_EPOCH};

use persona_spirit::{Command, Effect, SpiritActorRuntime, StoreLocation};
use signal_frame::SubscriptionTokenInner;
use signal_persona_spirit::{
    Certainty, Context, Entry, Kind, Observation, ObservationMode, ObserverFilter,
    ObserverSubscriptionToken, Operation as WorkingOperation, Quote, RecordQuery,
    Reply as WorkingReply, StateSubscriptionToken, Statement, StatementText, Subscription,
    SubscriptionToken, Summary, Topic,
};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome};

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
            "persona-spirit-sema-projection-{test_name}-{nanos}.redb"
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

fn entry(summary: &str) -> Entry {
    Entry {
        topic: Topic::new("workspace"),
        kind: Kind::Decision,
        summary: Summary::new(summary),
        context: Context::new("projection context"),
        certainty: Certainty::Maximum,
        quote: Quote::new("projection quote"),
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
    let request = WorkingOperation::Observe(Observation::Records(RecordQuery {
        topic: None,
        kind: None,
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
