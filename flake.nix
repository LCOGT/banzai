{
  description = "Banzai";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    devenv.url = "github:cachix/devenv";
    nix2container.url = "github:nlewo/nix2container";
    nix2container.inputs.nixpkgs.follows = "nixpkgs";
    mk-shell-bin.url = "github:rrbutani/nix-mk-shell-bin";
    devenv-k8s.url = "github:LCOGT/devenv-k8s";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
      ];
      systems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, ... }: {
        # Per-system attributes can be defined here. The self' and inputs'
        # module parameters provide easy access to attributes of the same
        # system.

        devenv.shells.default = {
          name = "banzai";

          imports = [
            # This is just like the imports in devenv.nix.
            # See https://devenv.sh/guides/using-with-flake-parts/#import-a-devenv-module
            # ./devenv-foo.nix
          ];

          # https://devenv.sh/reference/options/
          packages = [
            inputs'.devenv-k8s.devShells.default
          ];

          scripts.kind-create-cluster.exec = ''
            set -o errexit

            # Create the Kind cluster
            kind create cluster --config ./kind-cluster.yaml

            # Create registry container unless it already exists
            reg_name='kind-registry'
            reg_port='5001'

            if [ "$(docker inspect -f '{{.State.Running}}' "''${reg_name}" 2>/dev/null || true)" != 'true' ]; then
              docker run \
                -d --restart=always -p "127.0.0.1:''${reg_port}:5000" --network bridge --name "''${reg_name}" \
                registry:2
            fi

            # 3. Add the registry config to the nodes
            #
            # This is necessary because localhost resolves to loopback addresses that are
            # network-namespace local.
            # In other words: localhost in the container is not localhost on the host.
            #
            # We want a consistent name that works from both ends, so we tell containerd to
            # alias localhost:reg_port to the registry container when pulling images
            REGISTRY_DIR="/etc/containerd/certs.d/localhost:''${reg_port}"
            for node in $(kind get nodes); do
              docker exec "''${node}" mkdir -p "''${REGISTRY_DIR}"
              cat <<EOF | docker exec -i "''${node}" cp /dev/stdin "''${REGISTRY_DIR}/hosts.toml"
            [host."http://''${reg_name}:5000"]
            EOF
            done

            # Connect the registry to the cluster network if not already connected
            # This allows kind to bootstrap the network but ensures they're on the same network
            if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "''${reg_name}")" = 'null' ]; then
              docker network connect "kind" "''${reg_name}"
            fi

            # Document the local registry
            # https://github.com/kubernetes/enhancements/tree/master/keps/sig-cluster-lifecycle/generic/1755-communicating-a-local-registry
            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: ConfigMap
            metadata:
              name: local-registry-hosting
              namespace: kube-public
            data:
              localRegistryHosting.v1: |
                host: "localhost:''${reg_port}"
                help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
            EOF
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
