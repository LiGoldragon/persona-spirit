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
        daemonPackage = pkgs.runCommand "persona-spirit-daemon" { } ''
          mkdir -p "$out/bin"
          ln -s "${fullPackage}/bin/persona-spirit-daemon" "$out/bin/persona-spirit-daemon"
        '';
        splitPackageWitness = pkgs.runCommand "test-split-packages" { } ''
          test -x "${spiritPackage}/bin/spirit"
          test ! -e "${spiritPackage}/bin/persona-spirit-daemon"
          test -x "${daemonPackage}/bin/persona-spirit-daemon"
          test ! -e "${daemonPackage}/bin/spirit"
          touch "$out"
        '';
      in
      {
        packages = {
          default = spiritPackage;
          spirit = spiritPackage;
          persona-spirit-daemon = daemonPackage;
          full = fullPackage;
        };
        apps = {
          spirit = flake-utils.lib.mkApp {
            drv = spiritPackage;
            name = "spirit";
          };
          persona-spirit-daemon = flake-utils.lib.mkApp {
            drv = daemonPackage;
            name = "persona-spirit-daemon";
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
