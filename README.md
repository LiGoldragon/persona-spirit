# persona-spirit

Persona component for the psyche ↔ mind interface.

Current status: typed daemon foundation. The ordinary socket accepts
length-prefixed `signal-persona-spirit` frames over `signal-frame`; the owner
socket accepts `owner-signal-persona-spirit` frames over the owner contract;
the private upgrade socket accepts `signal-version-handover` frames for
handover marker, readiness, and completion.
The `spirit` CLI is a one-line `signal_frame::signal_cli!` client: it resolves
one argument as either a raw NOTA request or a path to a NOTA request file,
routes the record head through the generated working/owner contract table,
sends the corresponding `signal-frame` request to `PERSONA_SPIRIT_SOCKET` or
`PERSONA_SPIRIT_OWNER_SOCKET`, injects advisory caller context into the frame,
then renders the daemon's typed reply back to NOTA. It does not start an
in-process actor tree or open a store by itself.
Clients do not submit capture time; the daemon stamps records before storage
and exposes that time only in provenance replies.

The daemon actor tree persists `Record` operations, serves `Observe` reads
for records and the topic catalog, opens/retracts `Watch` subscriptions,
provisionally classifies raw `State`
statements into minimum-certainty records, and handles owner
lifecycle/bootstrap-policy requests.

The CLI can already capture and query daemon-backed typed intent records
when the caller supplies typed `Entry` records. Not implemented yet:
LLM-backed classification of raw psyche statements, spirit-to-mind owner
forwarding, live subscription event delivery, import of existing
`intent/*.nota` records, mirrored write replay on the upgrade socket, and the
workspace cutover that makes spirit storage canonical.
