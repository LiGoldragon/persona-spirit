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

## Query surface is description-first

Routine queries return record descriptions — topics, kind,
description, certainty, identifier. *"any agent reading [the intent file] is
going to get a lot of noise. Like the timestamps isn't always
useful. … most of the time just the summary is enough."*
Daemon-stamped date and time are available on demand through
provenance variants for verification.

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
NOTA belongs to the CLI/testing surface. The CLI may use a
running daemon by decoding one NOTA request into a typed
`SpiritRequest`, then sending it as a Signal frame.

## Constraints and actors must drive implementation

The psyche asked for implementation to continue as far as clear
intent allows, with intent scavenged into the repo and with
constraints, invariants, and actor logic planes implemented rather
than left as prose. *"implement all of the constraint tests, the
invariant tests, and only stop when you've done all that, unless
you need clarifications on some things."* For `persona-spirit`,
that means the raw storage/query path must already run through a
named Kameo actor tree with constraint tests that prove the path.

## Every actor has a schema

Every actor in `persona-spirit` carries its own `.schema` file
declaring two enums plus a universal variant: ACTION (the closed
set of things the actor can do when called) and RESPONSE (the
closed set of things the actor can say back). The schema engine
injects a universal `Unknown(String)` variant into every RESPONSE
enum automatically — the actor's **safety floor**. No matter what
arrives, the actor has a structurally-valid response.

This is a channel-contract per the workspace's
schemas-warrant-per-channel discipline. The recorder, observer,
supervisor, reading-actor, storage table set, and upgrade log each
get their own `.schema` file in the daemon crate. Storage and
internal channels live in `persona-spirit`; wire channels live in
`signal-persona-spirit` and `owner-signal-persona-spirit` as
before. *"When the psyche describes a major part of the system,
that description IS a warrant to create a schema for that part."*
Per record 668.

## Structure is schema; logic is Rust

Hand-written Rust is ONLY the engine logic — the decision-making
bodies inside actor methods. Everything STRUCTURAL emits from the
schema: NOTA-encoded form, rkyv binary form, Rust data type
definitions, field accessors, codec impls, dispatch traits. The
engine code consumes schema-emitted types and writes only logic. It
does not reinvent data structures.

Concretely: each actor's `handle(action: ActorAction) ->
ActorResponse` method is a closed `match` block. The action enum is
closed, so Rust enforces exhaustiveness at compile time. The body is
logic; the structure is the schema. There is no `Unknown` arm on the
action side; the universal Unknown lives on the response side as the
fallback any method body can return when the input is structurally
valid but unhandleable.

## One rkyv byte layout; two homes

The rkyv binary encoding lives in a single byte layout that survives
BOTH the database (sema body at rest in redb) and the wire (signal
movement between clients over sockets). Same bytes, two homes. NOTA
is the text-readable projection emitted at CLI read time or for
human inspection. This closes the schema-signal-sema trio at the
byte-encoding layer: **schema** specifies, **signal** moves, **sema**
holds. Per record 695.

## Database upgrades are auto-migration on load

Schema changes between versions follow the next/main/previous
vocabulary: NEXT is the in-progress authoring; MAIN is the
published baseline; PREVIOUS or LAST is the prior iteration. The
DB-side upgrade flow:

1. Author edits an actor or storage schema while writing NEXT; MAIN
   stays at the published baseline.
2. A schema-diff machine identifies what types are added, dropped,
   renamed, structurally changed.
3. The developer writes hand-written Rust bridge code per
   version-boundary — the `From`-impl per type that moved, in a
   `mod previous` / `mod next` pair (renamed from the older `mod
   historical` / `mod current_shape` per record 672).
4. A version-marker stored alongside the database tells the daemon
   which schema the persisted data was written under
   (`VersionMarker [u32 u32 u32]`).
5. The new daemon is recompiled with PREVIOUS available locally so
   the bridge can read both shapes.
6. Daemon startup reads the on-disk version marker; if previous, the
   migration method runs once, transforms data, updates the marker,
   persists, logs the outcome to an append-only upgrade log. When
   `main == next` at the same revision, the bridge body is elided
   per the empty-diff discipline.

The first implementation (per the `designer-schema-full-stack-
spirit-2026-05-25` branch) lives in
`src/schema_driven/storage.rs::SpiritStorageHandle::open` — a
three-branch match on the on-disk marker (None / Some(NEXT) /
Some(previous)). The upgrade log is in-memory pending cross-crate
schema-import resolution; the shapes match `spirit-upgrade-log.
schema`. Per record 696.

## Reading-actor + auto-tap

The daemon's response dispatch is itself an actor — the **reading
actor** — with its own schema. Its action vocabulary is
dispatch-by-response-type. Its fan-out targets always include a
`(Tap LogSinkSet WriteEntry)` row; the auto-tap to a logging
facility is **declared by the schema**, not enforced by runtime
convention. Every response is captured; nothing is invisible. Per
record 696 §5.

## Deployment — next, main, previous side-by-side

Spirit deploys **side-by-side** rather than as a destructive
replace. The user profile installs a versioned wrapper per tagged
release (`spirit-vX.Y.Z`), a `spirit-next` slot for the in-flight
authoring branch, and the unsuffixed `spirit` symlink points at the
current production target. Each versioned daemon has its own
segregated state directory under
`~/.local/state/persona-spirit/<version>/`, its own sockets, and
its own redb database — versioned daemons never share files.

This is the workspace's next/main/previous vocabulary applied at
the deployment layer: *what is being authored IS next*; *the
current published baseline IS main*; *previous is the prior release
retained for handover*. Cutover from one production version to the
next is an alias change, not a destructive replace — the older
daemon stays installed and reachable through its tag-suffixed
wrapper so handovers can be tested and reverted. The v0.2.0
deployment validated the pattern empirically: production stayed on
v0.1.0 while v0.2.0 ran in parallel for explicit testing through
`spirit-v0.2.0`. *"Migrate live Spirit to v0.2 now"* (psyche
2026-05-25) is the cutover trigger; the side-by-side substrate is
what makes that cutover safe.

## v0.3.0 wire discipline — multi-topic, description-only, terse, daemon-stamped

The v0.3.0 record shape carries one or more user-created `Topic`
values, one agent-clarified `Description`, a `Kind`, a
`Magnitude`, and daemon-stamped capture time. *"Spirit next entries
carry one clarified description and no verbatim field."* The
verbatim/context payloads from earlier shapes are gone; the agent's
job is to **clarify the psyche's wording into the description**
before recording. The forcing function is intent density: *intent
capture should become denser and less verbose; durable records
preserve clarified intent without large verbatim blocks that bloat
output and become lossy to work with.*

Four related v0.3.0 disciplines:

- **Daemon-stamped timestamps.** *"Spirit timestamp is
  daemon-stamped."* Clients do not supply capture time; the daemon
  is the single authority for when a record was accepted.
- **Terse acknowledgements.** *"Spirit record acknowledgements stay
  terse."* The wire reply to a `Record` is `(RecordAccepted N)` —
  no echo of the submitted content; the acknowledgement is
  token-cheap.
- **User-creatable topic strings.** Any new topic word a `Record`
  uses is registered by use; topics are not a pre-declared enum.
- **Multi-topic records and queries.** A `Record` carries a non-empty
  topic vector; filters can match one-or-more topics or every requested
  topic, and the topic catalog counts topic memberships.
- **Certainty filtering and removal-candidate review.** Stored certainty is
  required `Magnitude`, not an absent value; `Zero` is the lowest semantic
  certainty and Spirit interprets exact `Zero` as a removal candidate.
  `Minimum` remains weak but real intent. Record observations can filter
  certainty with `Any`, `Exact`, `AtMost`, or `AtLeast`. `ChangeCertainty`
  changes an existing record's certainty, including lowering it to `Zero`
  for review without removing the record.

## Daemon configuration — 9-field positional argument

The daemon binary takes one NOTA argument: a positional 9-field
record naming three Unix sockets (ordinary, owner, upgrade), one
redb database path, one magnitude limit, and four `None`-slot
extension points reserved for future configuration fields. The
CriomOS-home module is what authors this tuple per release; the
daemon's `ExecStart` line is the canonical witness. Future
configuration fields land by filling one of the `None` slots in the
contract crate, not by adding a flag. When the schema-driven
substrate matures, the configuration record will be schema-emitted
rather than hand-authored — but the contract shape (one positional
record argument) is stable.

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
- Primary workspace: `skills/spirit-cli.md` — deployed CLI
  invocation discipline and the wire-shape verification recipe.
