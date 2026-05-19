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
- Valid CLI requests emit a typed NOTA `SpiritReply`; until storage exists the
  reply is `SpiritRequestUnimplemented`.
- Runtime code does not invent intent-classification behavior.
- Spirit forwards authority to mind only through typed owner-signal contracts.
- Until the daemon runtime lands, binaries fail honestly with
  `RuntimeNotImplemented`.
