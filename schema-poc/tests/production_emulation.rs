//! Production-emulating tests (psyche 2026-05-26 + intent records 709,
//! 710 — orchestrator's task §"Match Spirit v0.3 capability —
//! production-emulating tests").
//!
//! Drive the POC daemon with v0.3-shape Operation values and assert
//! the Reply matches v0.3's behavior. Each test covers one v0.3 wire
//! operation; the table in /106 §"Production-emulating tests" maps
//! each v0.3 operation to its POC test name.

use signal_persona_spirit::{
    Description, Entry, Kind, Observation, ObservationMode, Operation, RecordQuery, Reply,
    Statement, StatementText, Topic, Topics,
};
use signal_sema::Magnitude;
use spirit_schema_poc::PocDaemon;

fn fixture_topic(value: &str) -> Topic {
    Topic::new(value)
}

fn fixture_entry() -> Entry {
    Entry {
        topics: Topics::new(vec![fixture_topic("workspace"), fixture_topic("spirit")]),
        kind: Kind::Decision,
        description: Description::new("schema-driven POC validates v0.3 wire shape"),
        certainty: Magnitude::Maximum,
    }
}

// --------------------------------------------------------------------------
// Record — the most-used v0.3 operation. The POC's reply mirrors the
// v0.3 contract: `RecordAccepted(RecordIdentifier)` with a freshly-
// minted identifier; no echo of submitted content.
// --------------------------------------------------------------------------

#[test]
fn poc_record_returns_record_accepted_with_fresh_identifier() {
    let daemon = PocDaemon::open_fresh();
    let operation = Operation::Record(fixture_entry());
    let reply = daemon.dispatch(operation);
    match reply {
        Reply::RecordAccepted(accepted) => {
            assert!(
                accepted.identifier().value() >= 1,
                "minted identifier starts at 1 per v0.3 contract; got {}",
                accepted.identifier().value()
            );
        }
        other => panic!("expected RecordAccepted, got {other:?}"),
    }
}

#[test]
fn poc_consecutive_records_get_monotonic_identifiers() {
    let daemon = PocDaemon::open_fresh();
    let first = match daemon.dispatch(Operation::Record(fixture_entry())) {
        Reply::RecordAccepted(accepted) => accepted.identifier().value(),
        other => panic!("expected RecordAccepted, got {other:?}"),
    };
    let second = match daemon.dispatch(Operation::Record(fixture_entry())) {
        Reply::RecordAccepted(accepted) => accepted.identifier().value(),
        other => panic!("expected RecordAccepted, got {other:?}"),
    };
    assert!(
        second > first,
        "v0.3 identifier mint is monotonic; got {first} then {second}",
    );
}

#[test]
fn poc_record_accepts_multi_topic_vec_per_record_702() {
    let daemon = PocDaemon::open_fresh();
    let entry = Entry {
        topics: Topics::new(vec![
            fixture_topic("workspace"),
            fixture_topic("spirit"),
            fixture_topic("signal"),
        ]),
        kind: Kind::Principle,
        description: Description::new("multi-topic record per intent record 702"),
        certainty: Magnitude::High,
    };
    let reply = daemon.dispatch(Operation::Record(entry));
    assert!(matches!(reply, Reply::RecordAccepted(_)));
}

// --------------------------------------------------------------------------
// Observe Topics — the v0.3 read operation that lists topic-with-count.
// The POC observer returns an empty TopicsObserved (production
// observer would walk the store).
// --------------------------------------------------------------------------

#[test]
fn poc_observe_topics_returns_topics_observed_reply() {
    let daemon = PocDaemon::open_fresh();
    let reply = daemon.dispatch(Operation::Observe(Observation::Topics));
    match reply {
        Reply::TopicsObserved(topics_observed) => {
            assert!(topics_observed.topics.is_empty());
        }
        other => panic!("expected TopicsObserved, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// Observe Records (DescriptionOnly) — v0.3 returns RecordsObserved with
// description-only entries; the POC mirrors the variant shape.
// --------------------------------------------------------------------------

#[test]
fn poc_observe_records_description_only_returns_records_observed_reply() {
    let daemon = PocDaemon::open_fresh();
    let query = RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::DescriptionOnly,
    };
    let reply = daemon.dispatch(Operation::Observe(Observation::Records(query)));
    match reply {
        Reply::RecordsObserved(observed) => {
            assert!(observed.records.is_empty());
        }
        other => panic!("expected RecordsObserved, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// Observe Records (WithProvenance) — v0.3 returns
// RecordProvenancesObserved; the POC's observer maps WithProvenance to
// the matching action.
// --------------------------------------------------------------------------

#[test]
fn poc_observe_records_with_provenance_returns_record_provenances_observed_reply() {
    let daemon = PocDaemon::open_fresh();
    let query = RecordQuery {
        topic: None,
        kind: None,
        mode: ObservationMode::WithProvenance,
    };
    let reply = daemon.dispatch(Operation::Observe(Observation::Records(query)));
    match reply {
        Reply::RecordProvenancesObserved(observed) => {
            assert!(observed.records.is_empty());
        }
        other => panic!("expected RecordProvenancesObserved, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// State — v0.3 currently returns RequestUnimplemented for free-form
// State submissions on the ordinary socket per the wire contract; the
// POC mirrors that.
// --------------------------------------------------------------------------

#[test]
fn poc_state_returns_request_unimplemented() {
    let daemon = PocDaemon::open_fresh();
    let operation = Operation::State(Statement {
        text: StatementText::new("free-form psyche statement"),
    });
    let reply = daemon.dispatch(operation);
    assert!(
        matches!(reply, Reply::RequestUnimplemented(_)),
        "expected RequestUnimplemented for State"
    );
}

// --------------------------------------------------------------------------
// Watch / Unwatch / Tap / Untap — v0.3 placeholders per
// /skills/spirit-cli.md §"Subscribe / unsubscribe". The POC returns
// RequestUnimplemented to mirror that behavior.
// --------------------------------------------------------------------------

#[test]
fn poc_watch_returns_request_unimplemented() {
    use signal_persona_spirit::{RecordSubscription, Subscription};
    let daemon = PocDaemon::open_fresh();
    let subscription = Subscription::Records(RecordSubscription {
        topic: None,
        mode: ObservationMode::DescriptionOnly,
    });
    let reply = daemon.dispatch(Operation::Watch(subscription));
    assert!(matches!(reply, Reply::RequestUnimplemented(_)));
}

#[test]
fn poc_unwatch_returns_request_unimplemented() {
    use signal_persona_spirit::{RecordSubscriptionToken, SubscriptionToken};
    let daemon = PocDaemon::open_fresh();
    let token =
        SubscriptionToken::Records(RecordSubscriptionToken { identifier: 0 });
    let reply = daemon.dispatch(Operation::Unwatch(token));
    assert!(matches!(reply, Reply::RequestUnimplemented(_)));
}

// --------------------------------------------------------------------------
// Universal-Unknown forward-compat floor on the wire side — the
// schema-driven Reply enum carries Unknown(String) injected by the
// composer. Verified at compile time by simply constructing it.
// --------------------------------------------------------------------------

#[test]
fn poc_schema_driven_wire_reply_carries_unknown_floor() {
    use signal_persona_spirit::spirit::Reply as SchemaReply;
    let _unknown_reply: SchemaReply =
        SchemaReply::Unknown("forward-compat for an operation we don't recognise".to_string());
}
