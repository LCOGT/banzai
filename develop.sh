#!/bin/sh

DEVENV_ROOT_FILE="$(mktemp)"
printf %s "$PWD" > "$DEVENV_ROOT_FILE"

nix develop --override-input devenv-root "file+file://$DEVENV_ROOT_FILE"
