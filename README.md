# persona-spirit

Persona component for the psyche ↔ mind interface.

Current status: typed daemon foundation. The ordinary socket accepts
length-prefixed `signal-persona-spirit` frames over `signal-frame`; the owner
socket accepts `owner-signal-persona-spirit` frames over the owner contract.
The `spirit` CLI is only a NOTA-to-Signal client: it resolves one argument
as either a raw NOTA request or a path to a NOTA request file, sends the
corresponding `signal-frame` request to the daemon named by
`PERSONA_SPIRIT_SOCKET`, then renders the daemon's typed reply back to NOTA.
It does not start an in-process actor tree or open a store by itself.

The daemon actor tree persists `Record` operations, serves `Observe` reads,
opens/retracts `Watch` subscriptions, provisionally classifies raw `State`
statements into minimum-certainty records, and handles owner
lifecycle/bootstrap-policy requests.

The CLI can already capture and query daemon-backed typed intent records
when the caller supplies typed `Entry` records. Not implemented yet:
LLM-backed classification of raw psyche statements, spirit-to-mind owner
forwarding, live subscription event delivery, import of existing
`intent/*.nota` records, and the workspace cutover that makes spirit storage
canonical.
