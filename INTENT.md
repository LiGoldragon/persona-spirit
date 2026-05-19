# INTENT — persona-spirit

*The psyche's intent for `persona-spirit`, synthesised from
`intent/persona.nota` in the primary workspace. Verbatim psyche
quotes in italics where the exact wording is load-bearing; the
surrounding prose is agent-composed from intent-log summaries.
Companion to `ARCHITECTURE.md` (structural shape) and `AGENTS.md`
(agent contract). Maintenance: `skills/repo-intent.md` and
`skills/intent-manifestation.md`.*

## What persona-spirit is

`persona-spirit` is the interface between `persona-mind` and the
psyche. It sits at the apex of the cognitive authority chain;
the supervisor has higher infrastructure permission only, for
process lifecycle. *"the apex, the most powerful part,
notwithstanding the supervisor, which only has higher
permission because it's an infrastructure component."* Spawned
last in the persona engine boot order — every component spirit
commands must be up first.

*"Persona is a meta AI system — the next evolutionary step in
AI engineering. … what drives humans, right, at the highest
level is spirit. That's what animates us."* Spirit is the
animating layer that ties persona to a real psyche. Without
spirit the persona is mechanism; with it, the system has
direction.

## Spirit owns mind

Spirit owns `persona-mind` in the authority graph: spirit
issues owner-Mutate against `owner-signal-persona-mind`. *"Yes,
of course spirit owns mind. … We'll have to develop and flesh
it out as it develops."* The apex relationship is settled; the
concrete verb set develops with implementation. Spirit-to-mind
wiring is not part of the first raw component slice.

## Bootstrap policy is the root intent

`bootstrap-policy.nota` is the root of spirit — the first
intent. *"the seed file is the first intent, right? The root
of spirit. Something like do no harm. Well, maybe not, but
basic stuff about how to live properly."* The content is to be
developed from foundational right-knowledge and right-action
principles in the spirit of the Bhagavad Gita; the research
arc is deferred. Current state is a minimal placeholder
indicating the destination.

## Spirit is dumb storage today

The first daemon is a dumb system — it takes typed input from
agents and stores it. *"the spirit is not a thinking thing. So
it doesn't make decisions. That's the agent's job. … just
going to be a dumb system. It just takes what it's given."*
Agents are the thinking layer — they are LLMs, they construct
typed records from speech-to-text input and submit them through
the CLI. Spirit trusts agent typing.

The eventual "spirit guardian" sub-actor — which judges
contradictions among intent records under the negation /
certainty-lowering / escalation lifecycle — is a future arc
that lands with the multi-agent auditing system. It is not part
of today's spirit.

## Query surface is summary-first

Routine queries return record summaries — topic, kind, summary,
certainty, identifier. *"any agent reading [the intent file] is
going to get a lot of noise. Like the timestamps isn't always
useful. … most of the time just the summary is enough."*
Verbatim, context, and timestamp are available on demand
through provenance variants for verification.

## Restatement is signal by repetition

The data model expresses intent intensity through repetition
rather than a per-record intensity field. Each psyche statement
is its own top-level record. *"separate records then. repeated
similar intents will mean stronger signal."* Dedup and
clustering of similar restatements is a query-time concern, not
a storage shape; recency comes from the latest matching
record's timestamp.

## Naming inside this crate

Intent is `persona-spirit`'s domain, so type names must not
prefix `Intent` — that's repetition of the crate's namespace.
*"the type intent record identifier is not good because spirit
deals with intent. So it's repetition to say intent record
identifier. At most it should be record identifier, if we even
say record."* The Entry-not-`IntentEntry` rule generalises:
every type currently prefixed `Intent` drops the prefix unless
removing it produces real ambiguity. `RecordIdentifier` is
spirit-internal — agents never supply one when asserting;
spirit mints from storage.

## Components ship in raw form first

*"we can use the components in the raw form like they don't
have to be talking to each other right away. We can let the
agents just use the components individually."* Spirit ships as
a standalone CLI + daemon + sema state first; spirit-to-mind
wiring lands after the raw component is working. Agents use
spirit's CLI for typed intent logging before any
component-to-component integration exists.

The daemon boundary is part of raw form. It accepts one typed
configuration argument, owns the long-lived Kameo root and
sema-engine store, and receives typed Signal frames from other
components. It does not parse NOTA request text on the socket;
NOTA belongs to the CLI/testing surface.

## Constraints and actors must drive implementation

The psyche asked for implementation to continue as far as clear
intent allows, with intent scavenged into the repo and with
constraints, invariants, and actor logic planes implemented rather
than left as prose. *"implement all of the constraint tests, the
invariant tests, and only stop when you've done all that, unless
you need clarifications on some things."* For `persona-spirit`,
that means the raw storage/query path must already run through a
named Kameo actor tree with constraint tests that prove the path.

## See also

- `ARCHITECTURE.md` — structural shape, state taxonomy, spawn
  order.
- `AGENTS.md` — agent contract for working in this repo.
- `bootstrap-policy.nota` — the first intent.
- `skills.md` — repo-specific capability notes.
- Primary workspace: `intent/persona.nota` — the verbatim
  psyche statements driving this synthesis.
- Primary workspace: `ESSENCE.md` §"Persona is meta-AI; spirit
  animates" — workspace-essence framing.
- Primary workspace: `INTENT.md` §"Persona is LLM-mediated
  end-to-end" + §"Persona components ship in raw form first" —
  workspace-wide persona principles.
- Primary workspace: `reports/designer/232-persona-spirit-new-component.md`
  — full design.
