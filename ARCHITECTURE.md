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

Policy state is seeded once from `bootstrap-policy.nota`, then changed only
through `owner-signal-persona-spirit`. Working state records captured intent,
psyche presence, pending clarification questions, and downstream owner-Mutate
audit once the runtime lands.

## Constraints

| Constraint | Witness |
|---|---|
| The CLI binary accepts exactly one argument. | `tests/boundary.rs` checks missing and extra arguments. |
| The daemon binary accepts exactly one argument. | `tests/boundary.rs` checks the shared argument parser. |
| The CLI type-checks one `signal-persona-spirit::SpiritRequest`. | `tests/boundary.rs` checks valid `Statement`, `Entry`, and `RecordObservation` requests. |
| `Entry` assertions persist a top-level record. | `persona_spirit_client_asserts_entry_and_mints_record_identifier` checks `RecordAccepted`. |
| Spirit mints `RecordIdentifier`; agents never submit it. | `persona_spirit_client_asserts_entry_and_mints_record_identifier` sends no identifier and receives one. |
| Repeated similar entries remain distinct records. | `persona_spirit_client_repeated_entries_remain_distinct_records` stores two matching summaries. |
| Summary queries do not include provenance. | `persona_spirit_client_persists_entries_for_later_summary_observation` checks `RecordsObserved`. |
| Provenance appears only when requested. | `persona_spirit_client_returns_provenance_only_when_requested` checks `RecordProvenancesObserved`. |
| No classifier or mind-forwarding behavior exists until its intent is clear. | Status section says this explicitly. |

## Code Map

```text
src/lib.rs                         — module entry
src/argument.rs                    — one-argument boundary
src/error.rs                       — typed error
src/runtime.rs                     — CLI request decoding + request dispatch
src/store.rs                       — sema-engine backed entry store and record queries
src/bin/persona-spirit.rs          — thin CLI binary
src/bin/persona-spirit-daemon.rs   — daemon binary
bootstrap-policy.nota              — first policy seed placeholder
tests/boundary.rs                  — argument-boundary witnesses
```

## Status

Implemented now:

- repo scaffold;
- daemon and CLI binary names;
- one-argument boundary parser;
- typed CLI request decoding for `signal-persona-spirit::SpiritRequest`;
- sema-engine backed `Entry` assertion;
- `RecordObservation` summary and provenance queries;
- typed `RequestUnimplemented` NOTA replies for behavior not built yet;
- dependency on the ordinary and owner spirit contracts.

Not implemented:

- daemon socket listener;
- Kameo actor tree;
- intent classifier;
- owner-Mutate forwarding to mind;
- filesystem intent projection.

The next implementation step needs the daemon configuration, socket shape, and
Kameo actor tree for spirit. Spirit-to-mind owner variants are not needed for
the current raw CLI/storage slice.
