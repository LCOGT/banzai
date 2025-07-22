{
  description = "Description for the project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    devenv-k8s.url = "github:LCOGT/devenv-k8s/v1";

    nixpkgs.follows = "devenv-k8s/nixpkgs";
    flake-parts.follows = "devenv-k8s/flake-parts";

    devenv-root = {
      url = "file+file:///dev/null";
      flake = false;
    };

  };

  nixConfig = {
    extra-substituters = [
      "https://devenv.cachix.org"
      "https://lco-public.cachix.org"
    ];

    extra-trusted-public-keys = [
      "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw="
      "lco-public.cachix.org-1:zSmLK7CkAehZ7QzTLZKt+5Y26Lr0w885GUB4GlT1SCg="
    ];
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv-k8s.flakeModules.default
      ];

      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, ... }: {
        # Per-system attributes can be defined here. The self' and inputs'
        # module parameters provide easy access to attributes of the same
        # system.

        # https://devenv.sh/basics/
        # Enter using `nix develop --impure`
        config.devenv.shells.default = {

          # use direnv without --impure
          devenv.root = let
            devenvRootFileContent = builtins.readFile inputs.devenv-root.outPath;
          in pkgs.lib.mkIf (devenvRootFileContent != "") devenvRootFileContent;

          # setup local development cluster
          devenv-k8s.local-cluster.enable = true;

          # https://devenv.sh/packages/
          packages = [

          ];

          env = {
            SKAFFOLD_CACHE_ARTIFACTS = "false";
          };

          enterShell = ''
            touch $DEVENV_ROOT/k8s/envs/local/secrets.env
          '';

          scripts.skaffold-banzai-init-db.exec = ''
            set -x

            kubectl -n banzai create job db-instrument-update-init --from=cronjob/db-instrument-update
            kubectl -n banzai create job db-bpm-update-init --from=cronjob/db-bpm-update

            kubectl -n banzai wait --for=condition=complete --timeout 1s job/db-instrument-update-init || kubectl -n banzai wait --for='jsonpath={.status.ready}=1' job/db-instrument-update-init
            kubectl -n banzai logs job/db-instrument-update-init --ignore-errors --pod-running-timeout=1m -c default -f

            kubectl -n banzai wait --for=condition=complete --timeout 1s job/db-bpm-update-init || kubectl -n banzai wait --for='jsonpath={.status.ready}=1' job/db-bpm-update-init
            kubectl -n banzai logs job/db-bpm-update-init --ignore-errors --pod-running-timeout=1m -c default -f

            kubectl -n banzai wait --for=condition=complete --timeout 10m job/db-instrument-update-init job/db-bpm-update-init
          '';

          scripts.skaffold-banzai-e2e-master-bias.exec = ''
            set -xe
            kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_bias "$@"
          '';

          scripts.skaffold-banzai-e2e-master-dark.exec = ''
            set -xe
            kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_dark "$@"
          '';

          scripts.skaffold-banzai-e2e-master-flat.exec = ''
            set -xe
            kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m master_flat "$@"
          '';

          scripts.skaffold-banzai-e2e-science-files.exec = ''
            set -xe
            kubectl -n banzai exec -it debug -- pytest -s --pyargs banzai --durations=0 -m science_files "$@"
          '';
        };
      };

      flake = {
        # The usual flake attributes can be defined here, including system-
        # agnostic ones like nixosModule and system-enumerating ones, although
        # those are more easily expressed in perSystem.

      };
    };
}
