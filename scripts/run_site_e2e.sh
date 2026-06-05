#!/usr/bin/env bash
set -euo pipefail

# Resolve paths relative to this script so developers can run it from any
# directory without accidentally pointing Docker Compose or pytest at the wrong
# checkout.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SITE_ENV_FILE="${REPO_ROOT}/banzai/tests/site_e2e/site_e2e.env"

die() {
  echo "run_site_e2e.sh: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

env_file_has_auth_token() {
  # The pytest fixtures load site_e2e.env themselves, but this wrapper checks
  # for AUTH_TOKEN up front so missing credentials fail before Docker starts.
  # This is only a friendly preflight: accept any non-comment AUTH_TOKEN line
  # with at least one non-space character after the equals sign.
  grep -Eq '^[[:space:]]*AUTH_TOKEN[[:space:]]*=[[:space:]]*[^[:space:]#]' "${SITE_ENV_FILE}"
}

# Fail before doing any Docker work when required local tooling is missing. This
# keeps setup problems separate from real site deployment/test failures.
require_command docker
require_command uv

if ! docker compose version >/dev/null 2>&1; then
  die "Docker Compose is required; expected 'docker compose' to be available"
fi

if [[ ! -f "${SITE_ENV_FILE}" ]]; then
  # Keep the setup instruction copy-pasteable and repo-relative; this is the
  # first command most new developers need.
  cat >&2 <<EOF
run_site_e2e.sh: missing site E2E environment file.

Create it with:
  cp banzai/tests/site_e2e/site_e2e.env.template banzai/tests/site_e2e/site_e2e.env

Then edit banzai/tests/site_e2e/site_e2e.env and set AUTH_TOKEN.
EOF
  exit 1
fi

if [[ -z "${AUTH_TOKEN:-}" ]] && ! env_file_has_auth_token; then
  die "AUTH_TOKEN must be set in the shell environment or in banzai/tests/site_e2e/site_e2e.env"
fi

cd "${REPO_ROOT}"

# Redis and RabbitMQ are shared developer dependencies, not owned by pytest's
# site_e2e fixtures. Start them here and intentionally leave them running.
echo "Starting shared Redis/RabbitMQ dependencies..."
docker compose -f docker-compose-dependencies.yml up -d

if [[ "$#" -gt 0 ]]; then
  # Treat arguments as the complete pytest argument list so advanced users can
  # select markers, files, verbosity, or debug flags without this script needing
  # its own option parser.
  echo "Running custom site E2E pytest command..."
  uv run pytest "$@"
else
  echo "Running full site E2E suite..."
  uv run pytest -m e2e_site banzai/tests/site_e2e/test_site_e2e.py -v -s
fi
