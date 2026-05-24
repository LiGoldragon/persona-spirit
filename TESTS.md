# Tests

`nix flake check` is the canonical gate for persona-spirit.

- `checks.<system>.test-engine-management-socket` runs
  `persona_spirit_daemon_serves_engine_management_socket_for_supervision`, which
  binds the engine-management socket from `DaemonConfiguration` and round-trips
  `Announce`, readiness, and health frames over
  `signal-engine-management`.
