# skills — persona-spirit

Read this before editing the spirit runtime.

## Required Context

- `~/primary/skills/component-triad.md`
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/kameo.md`
- `~/primary/skills/rust-discipline.md`
- this repo's `ARCHITECTURE.md`
- `signal-spirit/ARCHITECTURE.md`
- `meta-signal-spirit/ARCHITECTURE.md` (meta policy contract)

## Boundary

This repo owns the spirit runtime: daemon, CLI client, actor tree, sema-engine
state, classifier orchestration, and mind forwarding.

Contract records stay in `signal-spirit` and the meta policy contract
currently named `meta-signal-spirit`.

## Invariants

- CLI and daemon binaries take exactly one argument.
- The CLI peeks the NOTA request head and routes it through generated
  `signal-frame::signal_cli!` metadata from the working and meta policy
  contracts.
- The CLI decodes that argument as either a
  `signal-spirit::Operation` request or an
  `meta-signal-spirit::Operation` meta policy request, depending on
  the generated route decision.
- The daemon decodes that argument as `DaemonConfiguration`, selects the
  embedded or configured bootstrap-policy source, then binds one ordinary
  socket for `signal-spirit::Frame` values and one meta policy socket
  for the current `meta-signal-spirit::Frame` values.
- If `DaemonConfiguration` includes a handoff-control socket, the daemon
  connects to Persona's control socket and can receive public-client file
  descriptors over `SCM_RIGHTS`; each received descriptor is served as the same
  ordinary length-prefixed Signal stream. Persona is not on the byte path after
  the descriptor handoff. The handoff descriptor is an already-admitted client
  connection, so it can drain even if the daemon later closes direct public
  sockets during version handover.
- The CLI request path never opens `SpiritActorRuntime` directly. It decodes
  NOTA into the selected working or meta policy request type through
  `signal_frame::ClientShape`, injects advisory `Caller` context, sends a
  Signal frame to the selected daemon socket, and renders the daemon's Signal
  reply back to NOTA.
- When a daemon socket is selected, the CLI decodes NOTA once against that
  socket's contract and sends a Signal frame to the daemon rather than opening
  the store itself.
- `PERSONA_SPIRIT_SOCKET` configures the working socket for working requests;
  `PERSONA_SPIRIT_META_SOCKET` configures the meta policy socket.
- Signal-frame ingress submits typed requests directly to `SpiritRoot`; it does
  not go back through the NOTA decoder actor.
- Ordinary request execution passes through `signal-executor`: dispatch lowers
  the working contract operation into Spirit-local `Command`, executes through the Kameo actor
  planes as `CommandExecutor`, and publishes `signal-sema` observations.
- Spirit's current `CommandExecutor` implementation is degenerate-atomic:
  each accepted operation lowers to one command, and multi-operation batches
  and multi-command operation plans are rejected before any command runs. A
  future multi-command operation must add a real transaction boundary before
  it lands.
- The ordinary socket rejects meta policy frames; the meta policy socket rejects
  ordinary frames.
- Each named actor is data-bearing. Do not add public zero-sized actor nouns.
- Meta-signal lifecycle and identity requests route through `MetaPlane`, not
  through the ordinary text ingress or dispatch path.
- Bootstrap-policy reload routes from `MetaPlane` into `PolicyPlane` and
  returns `BootstrapPolicyReloaded` only after the policy source parses.
- A daemon configured with a bootstrap-policy path passes that path into
  `PolicyPlane`; it does not silently fall back to the embedded seed.
- `Entry` assertions persist one top-level record in the local sema-engine
  store and return `RecordAccepted`.
- `Entry` requests never carry client-provided capture time. They carry one or
  more topics, kind, one clarified description, certainty, and privacy.
  Privacy is directional `Magnitude`: `Zero` is open/public, higher values
  narrow the intended audience.
- Capture time is daemon-owned. `ClockPlane` stamps submitted entries before
  `RecordStore` persists them; provenance replies expose the daemon-produced
  bare `YYYY-MM-DD` date and bare `HH:MM:SS` time.
- Opaque epoch timestamp fields and parenthesized numeric date/time records are
  rejected at request decode time.
- `Entry` assertions pass through `RecordStore` and the sema-writer trace
  plane; queries pass through the sema-reader trace plane.
- `Observation::State` and `Observation::Questions` pass through
  `StatePlane`, not `RecordStore`.
- `SubscribeState` snapshots state through `StatePlane`, then opens the stream
  through `SubscriptionPlane`.
- `SubscribeRecords` snapshots descriptions through `RecordStore` and
  `SemaReader`, then opens the stream through `SubscriptionPlane`.
- Subscription retractions return typed close acknowledgements through
  `SubscriptionPlane`.
- `RecordObservation` queries return descriptions by default and provenance only
  when the caller asks for it. They filter by `Any`, `Partial`, or `Full`
  topic selection, optional kind, certainty selection, and recorded-time
  selection inside the daemon's read path. They also filter by privacy
  selection; the default selector is exact `Zero`, so elevated privacy records
  must be explicitly requested. Removal-candidate review is the exact `Zero`
  certainty query; `Minimum` remains weak but real intent.
  Qualitative recency depths (`Shallow`, `Recent`, `Deep`, `VeryDeep`) are
  applied after topic/kind/certainty matching and keep the newest matching
  records at the requested depth.
- Record subscription snapshots use the same privacy discipline as record
  observations: public `Watch(Records ...)` is exact `Zero`, while
  `Watch(PrivateRecords ...)` must carry an explicit `PrivacySelection`.
- `Observation::RecordIdentifiers` queries return descriptions or provenance
  for an exact `RecordIdentifier`. Identifier ranges are absent because random
  identifiers are not ordinal and do not carry recency.
- `ChangeCertainty(CertaintyChange)` mutates one stored intent entry's
  certainty through `RecordStore` and returns `CertaintyChanged`; setting
  certainty to `Zero` makes the record visible to removal-candidate review.
- `ChangeRecord(RecordChange)` mutates one stored intent entry's
  user-authored fields through `RecordStore`, preserves the
  `RecordIdentifier` and daemon-stamped provenance, and returns
  `RecordMutationApplied`.
- `CollectRemovalCandidates(RemovalCandidateCollection)` is the only
  bulk removal-candidate collection path. It must require exact-`Zero`
  certainty and exact-`Zero` privacy, preserve compact `RecordSummary`
  material before any retract, and return skipped candidates instead of
  retracting when archive database output fails. `OutputTarget::Print`
  writes no archive database; the typed `RemovalCandidatesCollected` reply is
  the capture material rendered by the CLI to the requested stream.
- Any release that changes persisted `SpiritStore` row shape must bump
  `SPIRIT_SCHEMA_VERSION`, keep the prior production row shape readable in
  `src/migration.rs`, expose a one-argument migration binary, and prove the
  bridge with tests before Home may point the unsuffixed `spirit` wrapper at a
  release. v0.4.1's bridge is `spirit-migrate-0-3-to-0-4`, which projects
  v0.3.0 records into the privacy-aware store with `privacy = Zero`. v0.5.0's
  bridge is `spirit-migrate-0-4-to-0-5`, which rewrites ordinal identifiers to
  random lowercase base36 identifiers and emits a NOTA hash-to-ordinal mapping
  table. v0.5.2's bridge is `spirit-migrate-0-5-to-0-5-2`, which preserves
  already-short random identifiers, remints copied long identifiers into the
  four-to-seven-character visible identifier surface, and emits a NOTA
  previous-to-current mapping table.
- `Remove(RecordIdentifier)` retracts one stored intent entry through
  `RecordStore` and returns `RecordRemoved`; the CLI never opens the
  database directly. Removed record identifiers are not reused. `RecordStore`
  mints the shortest collision-free lowercase base36 identifier code from four
  to seven characters, retries on live collision, and uses recorded time plus
  sema first-assert commit order for qualitative recency ties.
- Valid but unimplemented requests use `ReplyShaper` and do not touch
  `RecordStore`.
- Valid but unimplemented CLI requests emit a typed NOTA
  `RequestUnimplemented`.
- The flake exposes `packages.spirit` and `packages.persona-spirit-daemon`
  separately so a profile can install the CLI without the daemon or the daemon
  without relying on the default package.
- Runtime code does not invent intent-classification behavior.
- Spirit forwards authority to mind only through typed meta policy contracts.
- `persona-spirit-daemon` serves ordinary request/reply frames and meta policy
  request/reply frames on different sockets. Test-only bounded helpers must
  remove both sockets on shutdown.
