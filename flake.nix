{
  description = "persona-spirit - Persona psyche to mind interface component";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        bootstrapPolicyFilter = path: _type:
          builtins.baseNameOf path == "bootstrap-policy.nota";
        sourceFilter = path: type:
          (craneLib.filterCargoSources path type) || (bootstrapPolicyFilter path type);
        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = sourceFilter;
          name = "source";
        };
        cargoVendorDirectory = craneLib.vendorCargoDeps { inherit src; };
        commonArguments = {
          inherit src cargoVendorDirectory;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArguments;
        fullPackage = craneLib.buildPackage (commonArguments // { inherit cargoArtifacts; });
        spiritPackage = pkgs.runCommand "spirit" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/spirit" "$out/bin/spirit"
        '';
        spiritNextPackage = pkgs.runCommand "spirit-next" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/spirit-next" "$out/bin/spirit-next"
        '';
        daemonPackage = pkgs.runCommand "persona-spirit-daemon" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/persona-spirit-daemon" "$out/bin/persona-spirit-daemon"
        '';
        migrationPackage = pkgs.runCommand "spirit-migrate-0-1-to-0-2" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/spirit-migrate-0-1-to-0-2" "$out/bin/spirit-migrate-0-1-to-0-2"
        '';
        nextMigrationPackage = pkgs.runCommand "spirit-migrate-0-2-to-next" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/spirit-migrate-0-2-to-next" "$out/bin/spirit-migrate-0-2-to-next"
        '';
        splitPackageWitness = pkgs.runCommand "test-split-packages" { } ''
          test -x "${spiritPackage}/bin/spirit"
          test ! -e "${spiritPackage}/bin/persona-spirit-daemon"
          test -x "${spiritNextPackage}/bin/spirit-next"
          test ! -e "${spiritNextPackage}/bin/persona-spirit-daemon"
          test ! -e "${spiritNextPackage}/bin/spirit"
          test -x "${daemonPackage}/bin/persona-spirit-daemon"
          test ! -e "${daemonPackage}/bin/spirit"
          test ! -e "${daemonPackage}/bin/spirit-next"
          test -x "${migrationPackage}/bin/spirit-migrate-0-1-to-0-2"
          test ! -e "${migrationPackage}/bin/spirit"
          test ! -e "${migrationPackage}/bin/persona-spirit-daemon"
          test -x "${nextMigrationPackage}/bin/spirit-migrate-0-2-to-next"
          test ! -e "${nextMigrationPackage}/bin/spirit"
          test ! -e "${nextMigrationPackage}/bin/persona-spirit-daemon"
          touch "$out"
        '';
      in
      {
        packages = {
          default = spiritPackage;
          spirit = spiritPackage;
          spirit-next = spiritNextPackage;
          persona-spirit-daemon = daemonPackage;
          spirit-migrate-0-1-to-0-2 = migrationPackage;
          spirit-migrate-0-2-to-next = nextMigrationPackage;
          full = fullPackage;
        };
        apps = {
          spirit = flake-utils.lib.mkApp {
            drv = spiritPackage;
            name = "spirit";
          };
          spirit-next = flake-utils.lib.mkApp {
            drv = spiritNextPackage;
            name = "spirit-next";
          };
          persona-spirit-daemon = flake-utils.lib.mkApp {
            drv = daemonPackage;
            name = "persona-spirit-daemon";
          };
          spirit-migrate-0-1-to-0-2 = flake-utils.lib.mkApp {
            drv = migrationPackage;
            name = "spirit-migrate-0-1-to-0-2";
          };
          spirit-migrate-0-2-to-next = flake-utils.lib.mkApp {
            drv = nextMigrationPackage;
            name = "spirit-migrate-0-2-to-next";
          };
        };
        checks = {
          build = craneLib.cargoBuild (commonArguments // { inherit cargoArtifacts; });
          test = craneLib.cargoTest (commonArguments // { inherit cargoArtifacts; });
          test-boundary = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test boundary";
          });
          test-actor-runtime = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test actor_runtime";
          });
          test-daemon = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon";
          });
          test-migration = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test migration";
          });
          test-engine-management-socket = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_daemon_serves_engine_management_socket_for_supervision -- --exact";
          });
          test-handoff-control-serves-fd = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_daemon_serves_signal_frames_from_handed_off_file_descriptor -- --exact";
          });
          test-short-header-ingress-triage = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_daemon_rejects_mismatched_short_header_before_dispatch -- --exact";
          });
          test-design-d-persona-router-serves-spirit-cli = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test design_d_routing persona_spirit_cli_reaches_daemon_through_persona_handoff_router -- --exact";
          });
          test-design-d-selector-flip-routes-new-connections = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test design_d_routing persona_handoff_router_routes_new_connections_after_selector_flip_and_old_connections_drain -- --exact";
          });
          test-upgrade-completion-requires-readiness = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_upgrade_completion_requires_accepted_readiness -- --exact";
          });
          test-upgrade-readiness-rejects-drift = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_upgrade_readiness_rejects_commit_sequence_drift -- --exact";
          });
          test-upgrade-readiness-freezes-public-writes = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_upgrade_readiness_freezes_public_writes_until_completion -- --exact";
          });
          test-upgrade-recovery-reopens-public-writes = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_upgrade_recovery_reopens_public_writes_after_readiness -- --exact";
          });
          test-upgrade-mirror-applies-stamped-entry = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test daemon persona_spirit_upgrade_mirror_applies_stamped_entry_after_completion -- --exact";
          });
          test-sema-projection = craneLib.cargoTest (commonArguments // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test sema_projection";
          });
          test-split-packages = splitPackageWitness;
          doc = craneLib.cargoDoc (commonArguments // {
            inherit cargoArtifacts;
            RUSTDOCFLAGS = "-D warnings";
          });
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (commonArguments // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };
        devShells.default = pkgs.mkShell {
          name = "persona-spirit";
          packages = [ pkgs.jujutsu pkgs.pkg-config toolchain ];
        };
      });
}
