# skills — persona-spirit

Read this before editing the spirit runtime.

## Required Context

- `~/primary/skills/component-triad.md`
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/kameo.md`
- `~/primary/skills/rust-discipline.md`
- this repo's `ARCHITECTURE.md`
- `signal-persona-spirit/ARCHITECTURE.md`
- `owner-signal-persona-spirit/ARCHITECTURE.md`

## Boundary

This repo owns the spirit runtime: daemon, CLI client, actor tree, sema-engine
state, classifier orchestration, and mind forwarding.

Contract records stay in `signal-persona-spirit` and
`owner-signal-persona-spirit`.

## Invariants

- CLI and daemon binaries take exactly one argument.
- The CLI decodes that argument as a `signal-persona-spirit::SpiritRequest`.
- The daemon decodes that argument as `DaemonConfiguration`, selects the
  embedded or configured bootstrap-policy source, then binds one ordinary
  socket for `signal-persona-spirit::Frame` values and one owner socket for
  `owner-signal-persona-spirit::Frame` values.
- The CLI request path never opens `SpiritActorRuntime` directly. It decodes
  NOTA into `SpiritRequest`, sends a Signal frame to the daemon, and renders
  the daemon's Signal reply back to NOTA.
- When a daemon socket is selected, the CLI decodes NOTA once and sends a
  Signal frame to the daemon rather than opening the store itself.
- Signal-frame ingress submits typed requests directly to `SpiritRoot`; it does
  not go back through the NOTA decoder actor.
- Ordinary request execution passes through `signal-executor`: dispatch lowers
  `SpiritRequest` into Spirit-local `Command`, executes through the Kameo actor
  planes as `CommandExecutor`, and publishes `signal-sema` observations.
- The ordinary socket rejects owner frames; the owner socket rejects ordinary
  frames.
- Each named actor is data-bearing. Do not add public zero-sized actor nouns.
- Owner-signal lifecycle and identity requests route through `OwnerPlane`, not
  through the ordinary text ingress or dispatch path.
- Bootstrap-policy reload routes from `OwnerPlane` into `PolicyPlane` and
  returns `BootstrapPolicyReloaded` only after the policy source parses.
- A daemon configured with a bootstrap-policy path passes that path into
  `PolicyPlane`; it does not silently fall back to the embedded seed.
- `Entry` assertions persist one top-level record in the local sema-engine
  store and return `RecordAccepted`.
- `Entry` assertions pass through `RecordStore` and the sema-writer trace
  plane; queries pass through the sema-reader trace plane.
- `StateObservation` and `QuestionPending` pass through `StatePlane`, not
  `RecordStore`.
- `SubscribeState` snapshots state through `StatePlane`, then opens the stream
  through `SubscriptionPlane`.
- `SubscribeRecords` snapshots summaries through `RecordStore` and
  `SemaReader`, then opens the stream through `SubscriptionPlane`.
- Subscription retractions return typed close acknowledgements through
  `SubscriptionPlane`.
- `RecordObservation` queries return summaries by default and provenance only
  when the caller asks for it.
- Valid but unimplemented requests use `ReplyShaper` and do not touch
  `RecordStore`.
- Valid but unimplemented CLI requests emit a typed NOTA
  `RequestUnimplemented`.
- Runtime code does not invent intent-classification behavior.
- Spirit forwards authority to mind only through typed owner-signal contracts.
- `persona-spirit-daemon` serves ordinary request/reply frames and owner
  request/reply frames on different sockets. Test-only bounded helpers must
  remove both sockets on shutdown.
