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
- The CLI peeks the NOTA request head and routes it through generated
  `signal-frame::signal_cli!` metadata from the working and owner contracts.
- The CLI decodes that argument as either a
  `signal-persona-spirit::SpiritRequest` or an
  `owner-signal-persona-spirit::OwnerSpiritRequest`, depending on the
  generated route decision.
- The daemon decodes that argument as `DaemonConfiguration`, selects the
  embedded or configured bootstrap-policy source, then binds one ordinary
  socket for `signal-persona-spirit::Frame` values and one owner socket for
  `owner-signal-persona-spirit::Frame` values.
- The CLI request path never opens `SpiritActorRuntime` directly. It decodes
  NOTA into the selected working or owner request type, sends a Signal frame to
  the selected daemon socket, and renders the daemon's Signal reply back to
  NOTA.
- When a daemon socket is selected, the CLI decodes NOTA once against that
  socket's contract and sends a Signal frame to the daemon rather than opening
  the store itself.
- `PERSONA_SPIRIT_SOCKET` configures the working socket for working requests;
  `PERSONA_SPIRIT_OWNER_SOCKET` configures the owner socket for owner requests.
- Signal-frame ingress submits typed requests directly to `SpiritRoot`; it does
  not go back through the NOTA decoder actor.
- Ordinary request execution passes through `signal-executor`: dispatch lowers
  `SpiritRequest` into Spirit-local `Command`, executes through the Kameo actor
  planes as `CommandExecutor`, and publishes `signal-sema` observations.
- Spirit's current `CommandExecutor` implementation is degenerate-atomic:
  each accepted operation lowers to one command, and multi-operation batches
  and multi-command operation plans are rejected before any command runs. A
  future multi-command operation must add a real transaction boundary before
  it lands.
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
- `Entry` requests never carry client-provided capture time. They carry topic,
  kind, summary, context, certainty, and quote.
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
- The flake exposes `packages.spirit` and `packages.persona-spirit-daemon`
  separately so a profile can install the CLI without the daemon or the daemon
  without relying on the default package.
- Runtime code does not invent intent-classification behavior.
- Spirit forwards authority to mind only through typed owner-signal contracts.
- `persona-spirit-daemon` serves ordinary request/reply frames and owner
  request/reply frames on different sockets. Test-only bounded helpers must
  remove both sockets on shutdown.
