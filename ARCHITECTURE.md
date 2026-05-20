# persona-spirit — architecture

*Psyche ↔ mind interface; apex cognitive component of Persona.*

## Role

`persona-spirit` receives psyche statements, captures intent, and projects
typed intent into `persona-mind`. It is the cognitive authority above mind.
The supervisor has higher infrastructure permission only for process
lifecycle.

`persona-spirit` follows the component triad:

- `persona-spirit` — runtime daemon + thin CLI.
- `signal-persona-spirit` — ordinary peer-callable contract.
- `owner-signal-persona-spirit` — supervisor-only owner contract.

## Authority

```mermaid
flowchart TB
    psyche["psyche"]
    supervisor["persona supervisor"]
    spirit["persona-spirit"]
    mind["persona-mind"]
    orchestrate["persona-orchestrate"]

    psyche --> spirit
    supervisor --> spirit
    spirit --> mind
    mind --> orchestrate
```

Spirit is spawned last because it depends on the components it commands.

## State

`persona-spirit` owns one sema-engine database: `persona-spirit.redb`.

Policy state is seeded once from `bootstrap-policy.nota` unless
`DaemonConfiguration` names an explicit bootstrap-policy path. It is then
changed only through `owner-signal-persona-spirit`. Working state records
captured intent, psyche presence, pending clarification questions, and
downstream owner-Mutate audit once the runtime lands.

## Actor topology

The daemon keeps the Kameo actor tree alive behind two typed Unix sockets:

```mermaid
flowchart LR
    root["SpiritRoot"]
    owner["OwnerPlane"]
    policy["PolicyPlane"]
    ingress["IngressPhase"]
    decoder["NotaDecoder"]
    classifier["ClassifierPlane"]
    dispatch["DispatchPhase"]
    state["StatePlane"]
    subscription["SubscriptionPlane"]
    store["RecordStore"]
    shaper["ReplyShaper"]
    encoder["ReplyTextEncoder"]
    writer["SemaWriter trace"]
    reader["SemaReader trace"]

    root --> owner
    owner --> policy
    root --> ingress
    ingress --> decoder
    ingress --> dispatch
    dispatch --> classifier
    dispatch --> state
    dispatch --> subscription
    dispatch --> store
    dispatch --> shaper
    store --> writer
    store --> reader
    root --> encoder
```

`OwnerPlane` handles owner-only lifecycle and identity requests carried by
`owner-signal-persona-spirit`; it is not reachable through the ordinary text
ingress or dispatch path. `PolicyPlane` owns bootstrap-policy parsing and
reload state. `ClassifierPlane` owns the current conservative statement-to-record
policy. `RecordStore` owns `SpiritStore`, which owns the sema-engine
handle. It runs as the store plane. `StatePlane` owns current psyche state and
pending clarification questions. `SubscriptionPlane` owns subscription tokens
and live stream registrations. Request decoding, dispatch,
unimplemented-reply shaping, and NOTA reply rendering are separate actor
planes. `ActorTrace` is a runtime witness, not an audit log: tests assert the
expected actor path for each constraint.

The daemon socket path does not pretend RKYV Signal traffic is text. The
ordinary socket reads length-prefixed `signal-persona-spirit::Frame` values,
checks the `signal-frame::Request`, and submits each `SpiritRequest` directly to
`SpiritRoot` through the dispatch plane. The owner socket reads
length-prefixed `owner-signal-persona-spirit::Frame` values and submits each
`OwnerSpiritRequest` directly to `OwnerPlane`.

The `persona-spirit` CLI is not a second runtime. It decodes its single NOTA
argument into a `SpiritRequest`, sends that request to the daemon as a
length-prefixed `signal-frame` exchange on `PERSONA_SPIRIT_SOCKET`, and encodes
the daemon's `SpiritReply` back to NOTA. If no daemon socket is configured, the
CLI fails instead of opening a store or running the actor tree in-process.

## Constraints

| Constraint | Witness |
|---|---|
| The CLI binary accepts exactly one argument. | `tests/boundary.rs` checks missing and extra arguments. |
| The daemon binary accepts exactly one argument. | `tests/boundary.rs` checks the shared argument parser. |
| The CLI type-checks one `signal-persona-spirit::SpiritRequest`. | `tests/boundary.rs` checks valid `State`, `Record`, and `Observe` requests before daemon submission. |
| The CLI requires a daemon socket instead of using an in-process store fallback. | `persona_spirit_binary_requires_socket_environment` runs the binary without `PERSONA_SPIRIT_SOCKET` and expects failure. |
| The CLI path only translates NOTA to Signal frames and Signal replies to NOTA. | `persona_spirit_command_line_path_does_not_use_actor_runtime_directly` checks `runtime.rs` uses `SpiritRequestText`, `SpiritSignalClient`, and `SpiritReplyText`, and not `SpiritActorRuntime` or `StoreLocation`. |
| Kameo is the only actor runtime dependency. | `persona_spirit_uses_kameo_as_only_actor_runtime` scans the manifest. |
| Actor types are data-bearing, not public zero-sized actor nouns. | `persona_spirit_actor_types_are_data_bearing` checks each named actor has a struct body. |
| Raw `State` statements route through a classifier actor before storage. | `persona_spirit_state_statement_uses_classifier_before_store` checks `DispatchPhase` → `ClassifierPlane` → `RecordStore` → `SemaWriter`. |
| The provisional classifier preserves the raw quote and marks uncertainty. | `persona_spirit_client_classifies_statement_as_provisional_record` checks `Clarification` / `Minimum` output. |
| `Record` operations traverse root, ingress, decoder, dispatch, store, sema writer, and reply encoder. | `persona_spirit_entry_assertion_runs_through_actor_planes` checks `ActorTrace` ordering. |
| `Record` operations persist a top-level record. | `persona_spirit_client_asserts_entry_and_mints_record_identifier` checks `RecordAccepted`. |
| Spirit mints `RecordIdentifier`; agents never submit it. | `persona_spirit_client_asserts_entry_and_mints_record_identifier` sends no identifier and receives one. |
| Repeated similar entries remain distinct records. | `persona_spirit_client_repeated_entries_remain_distinct_records` stores two matching summaries. |
| Record observations use the read plane and not the write plane. | `persona_spirit_record_observation_uses_read_plane_without_write_plane` checks `SemaReader` without `SemaWriter`. |
| Psyche-state observations use a working-state plane, not record storage. | `persona_spirit_state_observation_uses_state_plane` checks `StatePlane` without `RecordStore`. |
| Pending-question observations use the working-state plane. | `persona_spirit_question_observation_uses_state_plane` and `persona_spirit_client_observes_empty_pending_questions` check the empty raw state. |
| State subscriptions snapshot current psyche state through the state plane before opening a stream. | `persona_spirit_state_subscription_uses_subscription_plane_after_state_snapshot` checks `StatePlane` before `SubscriptionPlane`. |
| Record subscriptions snapshot record summaries through the read plane before opening a stream. | `persona_spirit_record_subscription_uses_read_plane_then_subscription_plane` checks `SemaReader` before `SubscriptionPlane`. |
| Subscription retractions use the subscription plane and return typed retraction acknowledgements. | `persona_spirit_subscription_retractions_use_subscription_plane` checks `StateSubscriptionRetracted` and `RecordSubscriptionRetracted`. |
| Summary queries do not include provenance. | `persona_spirit_client_persists_entries_for_later_summary_observation` checks `RecordsObserved`. |
| Provenance appears only when requested. | `persona_spirit_client_returns_provenance_only_when_requested` checks `RecordProvenancesObserved`. |
| Valid unimplemented requests do not touch the store. | `persona_spirit_unimplemented_statement_uses_reply_shaper_not_store` checks `ReplyShaper` and absence of `RecordStore`. |
| Invalid NOTA keeps a typed decode error through the actor path. | `persona_spirit_invalid_text_keeps_typed_decode_error` checks `Error::InvalidSpiritRequest`. |
| Shutdown releases the store so a later runtime can reopen the same path. | `persona_spirit_shutdown_releases_store_for_restart` writes, stops, restarts, and reads. |
| Owner lifecycle requests route through `OwnerPlane`, not the ordinary dispatch path. | `persona_spirit_owner_lifecycle_orders_use_owner_plane` checks `Started` / `DrainedAndStopped` replies and no dispatch/store trace. |
| Owner identity requests route through `OwnerPlane`. | `persona_spirit_owner_identity_orders_use_owner_plane` checks register/retire replies. |
| Bootstrap-policy reload uses the policy plane. | `persona_spirit_bootstrap_policy_reload_uses_policy_plane` returns `BootstrapPolicyReloaded` and checks the `OwnerPlane` → `PolicyPlane` route. |
| Daemon configuration selects the bootstrap-policy source. | `persona_spirit_daemon_configuration_controls_bootstrap_policy_source` starts a daemon with an explicit policy path and reloads through the owner socket. |
| The daemon configuration is a single untagged NOTA struct record. | `persona_spirit_daemon_configuration_is_one_nota_record` round-trips the config and rejects a variant wrapper shape. |
| The daemon serves ordinary length-prefixed Signal frames through the actor root. | `persona_spirit_daemon_serves_signal_frames_through_actor_root` writes and reads through the ordinary Unix socket. |
| The daemon serves owner length-prefixed Signal frames through `OwnerPlane`. | `persona_spirit_daemon_serves_owner_signal_frames_through_owner_plane` writes and reads through the owner Unix socket. |
| The ordinary socket rejects owner Signal frames. | `persona_spirit_ordinary_socket_rejects_owner_signal_frames` writes an owner frame to the ordinary socket and expects decode rejection. |
| The owner socket rejects ordinary Signal frames. | `persona_spirit_owner_socket_rejects_ordinary_signal_frames` writes an ordinary frame to the owner socket and expects decode rejection. |
| Daemon shutdown removes both socket paths. | `persona_spirit_daemon_serves_signal_frames_through_actor_root` checks both ordinary and owner sockets are removed after bounded serving. |
| Signal-frame daemon ingress does not route through the NOTA decoder. | `persona_spirit_daemon_source_does_not_route_signal_frames_through_nota_decoder` checks the socket boundary calls `SubmitRequest`. |
| The CLI acts as a daemon client without bypassing Signal. | `persona_spirit_client_can_send_nota_request_to_running_daemon` decodes NOTA then sends a Signal frame to the socket. |
| No LLM classifier or mind-forwarding behavior exists until its intent is clear. | Status section says this explicitly. |

## Code Map

```text
src/lib.rs                         — module entry
src/argument.rs                    — one-argument boundary
src/daemon.rs                      — daemon configuration, bootstrap-policy source selection, socket binding, ordinary/owner frame codecs, signal clients
src/error.rs                       — typed error
src/runtime.rs                     — CLI boundary that converts NOTA request text to signal-frame request traffic and typed replies back to NOTA
src/store.rs                       — sema-engine backed entry store and record queries
src/actors/root.rs                 — Kameo root and blocking actor-runtime helper
src/actors/ingress.rs              — text ingress phase
src/actors/owner.rs                — owner-signal lifecycle and identity actor
src/actors/policy.rs               — bootstrap-policy parsing and reload actor
src/actors/decoder.rs              — strict NOTA request decoder actor
src/actors/classifier.rs           — conservative statement-to-record classifier actor
src/actors/dispatch.rs             — request dispatch actor
src/actors/state.rs                — psyche-state and pending-question working-state actor
src/actors/subscription.rs         — subscription token and stream registration actor
src/actors/store.rs                — sema-engine store actor
src/actors/reply.rs                — unimplemented reply shaper + NOTA reply encoder actors
src/actors/trace.rs                — actor-path witness values
src/actors/pipeline.rs             — typed in-process pipeline carriers
src/bin/persona-spirit.rs          — thin CLI binary
src/bin/persona-spirit-daemon.rs   — daemon binary
bootstrap-policy.nota              — first policy seed
tests/boundary.rs                  — argument-boundary witnesses
tests/actor_runtime.rs             — actor-path and architectural-truth witnesses
tests/daemon.rs                    — socket, signal-frame, and daemon-boundary witnesses
```

## Status

Implemented now:

- repo scaffold;
- daemon and CLI binary names;
- one-argument boundary parser;
- typed CLI request decoding for `signal-persona-spirit::SpiritRequest`;
- CLI daemon-client mode that requires `PERSONA_SPIRIT_SOCKET` and performs
  only NOTA request decoding, signal-frame submission, and NOTA reply encoding;
- provisional classifier for `State` statements that preserves the raw quote as
  a minimum-certainty `Clarification` record under topic `unclassified`;
- `persona-spirit-daemon` typed configuration and ordinary/owner Unix socket
  binding;
- length-prefixed RKYV ordinary Signal frame request/reply path over the
  ordinary daemon socket;
- length-prefixed RKYV owner Signal frame request/reply path over the owner
  daemon socket;
- CLI socket-client mode for a running daemon;
- actor trace witnesses for root, ingress, decode, dispatch, store, sema
  writer/reader, working state, reply shaping, and reply encoding;
- sema-engine backed `Record` operation;
- `Observe(Records(...))` summary and provenance queries;
- `Observe(State(...))` with default absent psyche state;
- `Observe(Questions(...))` with an empty pending-question set;
- `Watch(State(...))` and `Watch(Records(...))` with snapshot-open replies;
- `Unwatch(State(...))` and `Unwatch(Records(...))` with typed close acknowledgements;
- owner-signal start, drain/stop, register identity, and retire identity
  handling inside the actor tree;
- bootstrap-policy source selection from daemon configuration, parsing, and
  owner-signal reload acknowledgement through `PolicyPlane`;
- typed `RequestUnimplemented` NOTA replies for behavior not built yet;
- dependency on the ordinary and owner spirit contracts.

Not implemented:

- LLM-backed intent classification;
- owner-Mutate forwarding to mind;
- subscription event delivery;
- filesystem intent projection.

`persona-spirit` is therefore not ready to fully replace the ad-hoc intent-log
files. It can be used for daemon-backed typed capture and query experiments,
but file logs remain the canonical intent record until classification,
intent-log projection/cutover, live subscription delivery, and spirit-to-mind
owner-Mutate forwarding are implemented.

The next implementation step is subscription event delivery or spirit-to-mind
owner-Mutate forwarding. Spirit-to-mind owner variants are not needed for the
current raw CLI/storage/socket slice.
