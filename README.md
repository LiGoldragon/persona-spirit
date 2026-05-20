# persona-spirit

Persona component for the psyche ↔ mind interface.

Current status: typed daemon foundation. The ordinary socket accepts
length-prefixed `signal-persona-spirit` frames over `signal-frame`; the owner
socket accepts `owner-signal-persona-spirit` frames over `signal-core`. The CLI
can type-check one NOTA request or forward it to a running daemon. The actor
tree persists `Record` operations, serves `Observe` reads, opens/retracts
`Watch` subscriptions, and handles owner lifecycle/bootstrap-policy requests.

Not implemented yet: intent classification, spirit-to-mind owner-Mutate
forwarding, and live subscription event delivery.
