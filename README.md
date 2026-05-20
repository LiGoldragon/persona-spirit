# persona-spirit

Persona component for the psyche ↔ mind interface.

Current status: typed daemon foundation. The ordinary socket accepts
length-prefixed `signal-persona-spirit` frames over `signal-frame`; the owner
socket accepts `owner-signal-persona-spirit` frames over `signal-core`.
The `persona-spirit` CLI is only a NOTA-to-Signal client: it decodes one NOTA
request, sends the corresponding `signal-frame` request to the daemon named by
`PERSONA_SPIRIT_SOCKET`, then renders the daemon's typed reply back to NOTA.
It does not start an in-process actor tree or open a store by itself.

The daemon actor tree persists `Record` operations, serves `Observe` reads,
opens/retracts `Watch` subscriptions, provisionally classifies raw `State`
statements into minimum-certainty records, and handles owner
lifecycle/bootstrap-policy requests.

Not implemented yet: LLM-backed classification, spirit-to-mind owner-Mutate
forwarding, live subscription event delivery, and full replacement of the
manual intent-log files. The CLI can already capture/query daemon-backed typed
records, but the ad-hoc files remain canonical until the missing intent-log
semantics and cutover path land.
