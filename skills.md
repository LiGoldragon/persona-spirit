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
- The daemon decodes that argument as `DaemonConfiguration`, then accepts
  length-prefixed `signal-persona-spirit::Frame` values on its Unix socket.
- The CLI request path runs through `SpiritActorRuntime` and the Kameo actor
  tree before it can produce a reply.
- When a daemon socket is selected, the CLI decodes NOTA once and sends a
  Signal frame to the daemon rather than opening the store itself.
- Signal-frame ingress submits typed requests directly to `SpiritRoot`; it does
  not go back through the NOTA decoder actor.
- Each named actor is data-bearing. Do not add public zero-sized actor nouns.
- `Entry` assertions persist one top-level record in the local sema-engine
  store and return `RecordAccepted`.
- `Entry` assertions pass through `RecordStore` and the sema-writer trace
  plane; queries pass through the sema-reader trace plane.
- `RecordObservation` queries return summaries by default and provenance only
  when the caller asks for it.
- Valid but unimplemented requests use `ReplyShaper` and do not touch
  `RecordStore`.
- Valid but unimplemented CLI requests emit a typed NOTA
  `RequestUnimplemented`.
- Runtime code does not invent intent-classification behavior.
- Spirit forwards authority to mind only through typed owner-signal contracts.
- `persona-spirit-daemon` currently serves ordinary request/reply frames and
  stops only when its process is stopped or test code calls bounded serving
  helpers.
