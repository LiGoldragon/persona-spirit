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
- The CLI request path runs through `SpiritActorRuntime` and the Kameo actor
  tree before it can produce a reply.
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
- Until the daemon socket runtime lands, `persona-spirit-daemon` fails honestly
  with `RuntimeNotImplemented`.
