use persona_spirit::{Command, Effect, SpiritActorRuntime, StoreLocation, spirit_runtime};
use signal_frame::{LogVariant, ShortHeader};
use signal_persona_spirit::{
    Description, Entry, Kind, Observation, ObservationMode, Operation as WorkingOperation,
    RecordQuery, Reply as WorkingReply, Topic, Topics,
};
use signal_sema::Magnitude;

fn live_entry(description: &str) -> Entry {
    Entry {
        topics: Topics::single(Topic::new("schema")),
        kind: Kind::Decision,
        description: Description::new(description),
        certainty: Magnitude::Maximum,
    }
}

fn schema_entry(description: &str) -> spirit_runtime::Entry {
    spirit_runtime::Entry {
        topics: spirit_runtime::Topics(vec![spirit_runtime::Topic("schema".to_string())]),
        kind: spirit_runtime::Kind::Decision,
        description: spirit_runtime::Description(description.to_string()),
        certainty: spirit_runtime::Magnitude::Maximum,
    }
}

fn temporary_store(test_name: &str) -> StoreLocation {
    let mut path = std::env::temp_dir();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    path.push(format!(
        "persona-spirit-full-schema-{test_name}-{nonce}.redb"
    ));
    StoreLocation::new(path)
}

fn reply_variant(reply: &WorkingReply) -> &'static str {
    match reply {
        WorkingReply::RecordAccepted(_) => "RecordAccepted",
        WorkingReply::StateObserved(_) => "StateObserved",
        WorkingReply::RecordsObserved(_) => "RecordsObserved",
        WorkingReply::RecordProvenancesObserved(_) => "RecordProvenancesObserved",
        WorkingReply::TopicsObserved(_) => "TopicsObserved",
        WorkingReply::QuestionsObserved(_) => "QuestionsObserved",
        WorkingReply::SubscriptionOpened(_) => "SubscriptionOpened",
        WorkingReply::SubscriptionRetracted(_) => "SubscriptionRetracted",
        WorkingReply::ObserverSubscriptionOpened(_) => "ObserverSubscriptionOpened",
        WorkingReply::RequestUnimplemented(_) => "RequestUnimplemented",
    }
}

fn fan_out_contains_reply(effect: &str, reply: &str) -> bool {
    let fan_out = spirit_runtime::AuthoredEffectTable::fan_out_for_effect(effect)
        .expect("effect has schema-authored fan-out");
    fan_out.outputs.iter().any(|output| {
        matches!(
            output,
            spirit_runtime::AuthoredFanOutOutput::Reply { variant } if *variant == reply
        )
    })
}

#[test]
fn schema_runtime_declares_current_store_table() {
    assert_eq!(
        spirit_runtime::StorageDescriptor::table_type_for("Records"),
        Some("RecordsTable")
    );
    assert_eq!(spirit_runtime::StorageDescriptor::TABLE_COUNT, 1);
}

#[test]
fn schema_runtime_declares_project_emit_and_response_routes() {
    let operation = spirit_runtime::SemaTurn::Project(
        spirit_runtime::ProjectEndpoint::AssertEntry(schema_entry("schema route")),
    );
    let route = spirit_runtime::route_for_short_header(
        spirit_runtime::Leg::Sema,
        ShortHeader::new(operation.log_variant()),
    )
    .expect("schema route exists");

    assert_eq!(route.root, "Project");
    assert_eq!(route.endpoint, "AssertEntry");
    assert_eq!(
        route.body,
        spirit_runtime::RouteBodyDescriptor::Type("Entry")
    );

    assert!(
        spirit_runtime::ROUTES
            .iter()
            .any(|route| route.leg == spirit_runtime::Leg::Sema
                && route.root == "Emit"
                && route.endpoint == "RecordAccepted")
    );
    assert!(
        spirit_runtime::ROUTES
            .iter()
            .any(|route| route.leg == spirit_runtime::Leg::Sema
                && route.root == "Respond"
                && route.endpoint == "RecordAccepted")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_record_turn_matches_schema_effect_and_reply_tables() {
    let runtime = SpiritActorRuntime::start(temporary_store("record"))
        .await
        .expect("runtime starts");
    let request = WorkingOperation::Record(live_entry("schema effect table"));
    let command = Command::from_request(request.clone()).expect("request lowers");
    let action = command.schema_action();

    let schema_effect = spirit_runtime::AuthoredEffectTable::effect_for_action(action)
        .expect("schema maps command action to effect");

    let runtime_reply = runtime
        .submit_request(request)
        .await
        .expect("runtime records entry");
    let reply = runtime_reply.into_reply();
    let effect = Effect::from_reply(reply.clone());

    assert_eq!(schema_effect, effect.schema_effect());
    assert!(fan_out_contains_reply(schema_effect, reply_variant(&reply)));
    assert_eq!(
        command.schema_declared_effect(),
        Some(effect.schema_effect())
    );
    assert!(effect.schema_declared_fan_out().is_some());

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_query_turn_matches_schema_effect_and_reply_tables() {
    let runtime = SpiritActorRuntime::start(temporary_store("query"))
        .await
        .expect("runtime starts");
    runtime
        .submit_request(WorkingOperation::Record(live_entry("query seed")))
        .await
        .expect("seed record accepted");

    let request = WorkingOperation::Observe(Observation::Records(RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::DescriptionOnly,
    }));
    let command = Command::from_request(request.clone()).expect("request lowers");
    let action = command.schema_action();

    let schema_effect = spirit_runtime::AuthoredEffectTable::effect_for_action(action)
        .expect("schema maps query command action to effect");

    let runtime_reply = runtime
        .submit_request(request)
        .await
        .expect("runtime observes records");
    let reply = runtime_reply.into_reply();
    let effect = Effect::from_reply(reply.clone());

    assert_eq!(schema_effect, effect.schema_effect());
    assert!(fan_out_contains_reply(schema_effect, reply_variant(&reply)));
    assert_eq!(
        command.schema_declared_effect(),
        Some(effect.schema_effect())
    );
    assert!(effect.schema_declared_fan_out().is_some());

    runtime.stop().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_provenance_query_turn_uses_distinct_schema_effect() {
    let runtime = SpiritActorRuntime::start(temporary_store("provenance-query"))
        .await
        .expect("runtime starts");
    runtime
        .submit_request(WorkingOperation::Record(live_entry("provenance seed")))
        .await
        .expect("seed record accepted");

    let request = WorkingOperation::Observe(Observation::Records(RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::WithProvenance,
    }));
    let command = Command::from_request(request.clone()).expect("request lowers");
    let action = command.schema_action();

    assert_eq!(action, "ReadRecordProvenances");
    let schema_effect = spirit_runtime::AuthoredEffectTable::effect_for_action(action)
        .expect("schema maps provenance query command action to effect");

    let runtime_reply = runtime
        .submit_request(request)
        .await
        .expect("runtime observes records with provenance");
    let reply = runtime_reply.into_reply();
    let effect = Effect::from_reply(reply.clone());

    assert_eq!(schema_effect, "RecordProvenancesObserved");
    assert_eq!(schema_effect, effect.schema_effect());
    assert!(fan_out_contains_reply(schema_effect, reply_variant(&reply)));
    assert_eq!(
        command.schema_declared_effect(),
        Some(effect.schema_effect())
    );
    assert!(effect.schema_declared_fan_out().is_some());

    runtime.stop().await.expect("runtime stops");
}
