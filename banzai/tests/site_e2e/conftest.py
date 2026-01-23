"""
Pytest fixtures for site E2E tests.

These fixtures manage the full site deployment lifecycle:
1. Start and initialize publication database
2. Start site deployment (depends on publication DB being ready)
3. Clean up all resources after tests complete

CRITICAL: Publication must be fully set up before site deployment starts,
as the site's init container will attempt to subscribe to the publication.
"""

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from banzai.tests.site_e2e.utils import populate_publication
from banzai.cache import replication


# Directory where this conftest.py lives
SITE_E2E_DIR = Path(__file__).parent.resolve()


def load_env_file(env_path):
    """
    Load environment variables from a .env file.

    Simple parser that handles KEY=VALUE format.
    Does not override existing environment variables.
    """
    if not env_path.exists():
        return

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            # Parse KEY=VALUE
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip()
                # Don't override existing env vars
                if key and key not in os.environ:
                    os.environ[key] = value


# Load environment variables from site_e2e.env
ENV_FILE = SITE_E2E_DIR / "site_e2e.env"
load_env_file(ENV_FILE)


# ---------------------------------------------------------------------------
# Helper fixtures for configuration values
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def publication_db_address():
    """Return the publication database connection string."""
    return os.environ.get(
        "PUBLICATION_DB_ADDRESS",
        "postgresql://banzai:banzai_test@localhost:5433/banzai_test"
    )


@pytest.fixture(scope="session")
def local_db_address():
    """Return the local database connection string (host port 5442)."""
    return os.environ.get(
        "LOCAL_DB_ADDRESS",
        "postgresql://banzai@localhost:5442/banzai_local"
    )


@pytest.fixture(scope="session")
def cache_dir():
    """Return the path to the calibration cache directory.

    Note: This is at the repo root because docker-compose runs from there
    and HOST_DATA_DIR=./data is relative to the repo root.
    """
    repo_root = SITE_E2E_DIR.parents[2]
    return repo_root / "data" / "calibrations"


@pytest.fixture(scope="session")
def data_dir():
    """Return the path to the test data directory.

    Note: This is at the repo root because docker-compose runs from there
    and HOST_DATA_DIR=./data is relative to the repo root.
    """
    repo_root = SITE_E2E_DIR.parents[2]
    return repo_root / "data"


@pytest.fixture(scope="session")
def site_compose_file():
    """Return the path to the site docker-compose file."""
    repo_root = SITE_E2E_DIR.parents[2]
    return repo_root / "docker-compose-site.yml"


@pytest.fixture(scope="session")
def site_env_file():
    """Return the path to the site environment file."""
    return SITE_E2E_DIR / "site_e2e.env"


@pytest.fixture(scope="session")
def archive_api_url():
    """Return the archive API URL."""
    return os.environ.get("API_ROOT", "https://archive-api.lco.global/")


@pytest.fixture(scope="session")
def auth_token():
    """
    Return the AUTH_TOKEN from environment.

    Skips tests if AUTH_TOKEN is not set.
    """
    token = os.environ.get("AUTH_TOKEN")
    if not token:
        pytest.skip("AUTH_TOKEN environment variable required for site E2E tests")
    return token


# ---------------------------------------------------------------------------
# Docker Compose helpers
# ---------------------------------------------------------------------------

def run_docker_compose(compose_file, *args, cwd=None, env=None):
    """
    Run a docker compose command.

    Parameters
    ----------
    compose_file : str or Path
        Path to the docker-compose.yml file
    *args : str
        Additional arguments to pass to docker compose
    cwd : str or Path, optional
        Working directory for the command
    env : dict, optional
        Environment variables to pass to the command

    Returns
    -------
    subprocess.CompletedProcess
        The result of the command
    """
    cmd = ["docker", "compose", "-f", str(compose_file)] + list(args)
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=merged_env,
        capture_output=True,
        text=True,
        check=False
    )


def wait_for_healthy(compose_file, service_name=None, timeout=60, cwd=None, env=None):
    """
    Wait for docker compose services to be healthy.

    Parameters
    ----------
    compose_file : str or Path
        Path to the docker-compose.yml file
    service_name : str, optional
        Specific service to wait for (default: all services)
    timeout : int
        Maximum seconds to wait
    cwd : str or Path, optional
        Working directory for the command
    env : dict, optional
        Environment variables to pass to the command

    Returns
    -------
    bool
        True if services are healthy, False if timeout reached
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = run_docker_compose(compose_file, "ps", "--format", "json", cwd=cwd, env=env)

        if result.returncode == 0 and result.stdout.strip():
            # Parse the JSON output to check service health
            import json
            try:
                # docker compose ps --format json outputs one JSON object per line
                lines = result.stdout.strip().split('\n')
                all_healthy = True
                found_service = False

                for line in lines:
                    if not line.strip():
                        continue
                    service_info = json.loads(line)
                    svc_name = service_info.get("Service", service_info.get("Name", ""))

                    # If we're looking for a specific service, skip others
                    if service_name and service_name not in svc_name:
                        continue

                    found_service = True
                    state = service_info.get("State", "")
                    health = service_info.get("Health", "")

                    # Service is healthy if running and either no healthcheck or healthy
                    if state != "running":
                        all_healthy = False
                    elif health and health != "healthy":
                        all_healthy = False

                if found_service and all_healthy:
                    return True

            except (json.JSONDecodeError, KeyError):
                pass

        time.sleep(2)

    return False


def wait_for_service_exit(compose_file, service_name, expected_code=0, timeout=120, cwd=None, env=None):
    """
    Wait for a one-shot service (like init containers) to complete.

    Parameters
    ----------
    compose_file : str or Path
        Path to the docker-compose.yml file
    service_name : str
        Name of the service to wait for
    expected_code : int
        Expected exit code (default: 0)
    timeout : int
        Maximum seconds to wait
    cwd : str or Path, optional
        Working directory for the command
    env : dict, optional
        Environment variables to pass to the command

    Returns
    -------
    bool
        True if service exited with expected code, False otherwise
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = run_docker_compose(compose_file, "ps", "-a", "--format", "json", cwd=cwd, env=env)

        if result.returncode == 0 and result.stdout.strip():
            import json
            try:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if not line.strip():
                        continue
                    service_info = json.loads(line)
                    svc_name = service_info.get("Service", service_info.get("Name", ""))

                    if service_name in svc_name:
                        state = service_info.get("State", "")
                        exit_code = service_info.get("ExitCode", -1)

                        if state == "exited":
                            return exit_code == expected_code

            except (json.JSONDecodeError, KeyError):
                pass

        time.sleep(2)

    return False


# ---------------------------------------------------------------------------
# Main fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def publication_db(publication_db_address):
    """
    Start and initialize the publication database.

    This fixture:
    1. Starts the publication database via docker compose
    2. Waits for it to be healthy
    3. Creates the schema and inserts initial test data

    Yields the database address for use in tests.
    """
    compose_file = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"

    print("\n[Publication DB] Starting...")
    result = run_docker_compose(compose_file, "up", "-d")
    if result.returncode != 0:
        pytest.fail(f"Failed to start publication DB: {result.stderr}")

    print("[Publication DB] Waiting for healthy...")
    if not wait_for_healthy(compose_file, "postgres", timeout=60):
        # Capture logs for debugging
        logs = run_docker_compose(compose_file, "logs")
        pytest.fail(f"Publication DB did not become healthy. Logs:\n{logs.stdout}\n{logs.stderr}")

    print("[Publication DB] Inserting initial data...")
    try:
        populate_publication.insert_initial_data(publication_db_address)
    except Exception as e:
        pytest.fail(f"Failed to populate publication DB: {e}")

    print("[Publication DB] Ready")
    yield publication_db_address


@pytest.fixture(scope="session")
def site_deployment(publication_db, data_dir):
    """
    Start the site deployment.

    This fixture depends on publication_db to ensure the publication is
    fully set up before the site's init container tries to subscribe.

    Starts the full site deployment via docker-compose-site.yml.
    """
    # The docker-compose-site.yml is at the repo root
    repo_root = SITE_E2E_DIR.parents[2]  # banzai/tests/site_e2e -> banzai/tests -> banzai -> repo root
    compose_file = repo_root / "docker-compose-site.yml"
    env_file = SITE_E2E_DIR / "site_e2e.env"

    if not compose_file.exists():
        pytest.fail(f"docker-compose-site.yml not found at {compose_file}")

    if not env_file.exists():
        pytest.fail(
            f"site_e2e.env not found at {env_file}. "
            f"Copy site_e2e.env.template and fill in required values."
        )

    # Ensure data directories exist
    (data_dir / "calibrations").mkdir(parents=True, exist_ok=True)
    (data_dir / "output").mkdir(parents=True, exist_ok=True)
    (data_dir / "postgres").mkdir(parents=True, exist_ok=True)

    print("\n[Site Deployment] Starting...")
    result = run_docker_compose(
        compose_file,
        "--env-file", str(env_file),
        "up", "-d", "--build",
        cwd=repo_root
    )
    if result.returncode != 0:
        pytest.fail(f"Failed to start site deployment: {result.stderr}")

    # Wait for the init container to complete successfully
    print("[Site Deployment] Waiting for cache-init to complete...")
    if not wait_for_service_exit(
        compose_file,
        "banzai-cache-init",
        expected_code=0,
        timeout=180,
        cwd=repo_root,
        env={"ENV_FILE": str(env_file)}
    ):
        # Capture logs for debugging
        logs = run_docker_compose(compose_file, "--env-file", str(env_file), "logs", "banzai-cache-init", cwd=repo_root)
        pytest.fail(f"Cache init did not complete successfully. Logs:\n{logs.stdout}\n{logs.stderr}")

    # Wait for core services to be healthy
    print("[Site Deployment] Waiting for services to be healthy...")
    if not wait_for_healthy(
        compose_file,
        timeout=120,
        cwd=repo_root,
        env={"ENV_FILE": str(env_file)}
    ):
        logs = run_docker_compose(compose_file, "--env-file", str(env_file), "logs", cwd=repo_root)
        pytest.fail(f"Site services did not become healthy. Logs:\n{logs.stdout}\n{logs.stderr}")

    print("[Site Deployment] Ready")
    yield


@pytest.fixture(scope="session", autouse=True)
def cleanup(request, local_db_address, data_dir):
    """
    Cleanup fixture that runs after all tests complete.

    Cleanup order is critical:
    1. Drop subscription from local DB (releases replication slot on publication)
    2. docker compose down -v for site deployment
    3. docker compose down -v for publication_db
    4. Remove data/ directory contents
    """
    # No setup needed, just yield
    yield

    print("\n[Cleanup] Starting cleanup...")

    repo_root = SITE_E2E_DIR.parents[2]
    site_compose = repo_root / "docker-compose-site.yml"
    pub_compose = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"
    env_file = SITE_E2E_DIR / "site_e2e.env"
    site_id = os.environ.get("SITE_ID", "lsc")

    # 1. Drop subscription from local DB (if it exists)
    print("[Cleanup] Dropping subscription...")
    try:
        subscription_name = f"banzai_{site_id}_sub"
        replication.drop_subscription(local_db_address, subscription_name, drop_slot=True)
    except Exception as e:
        print(f"[Cleanup] Warning: Failed to drop subscription: {e}")

    # 2. docker compose down -v for site deployment
    print("[Cleanup] Stopping site deployment...")
    if site_compose.exists():
        run_docker_compose(
            site_compose,
            "--env-file", str(env_file),
            "down", "-v", "--remove-orphans",
            cwd=repo_root
        )

    # 3. docker compose down -v for publication_db
    print("[Cleanup] Stopping publication database...")
    if pub_compose.exists():
        run_docker_compose(pub_compose, "down", "-v", "--remove-orphans")

    # 4. Remove data/ directory contents (repo_root/data/ for site deployment)
    print("[Cleanup] Removing data directory contents...")
    if data_dir.exists():
        for item in data_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)

    # 5. Remove publication DB postgres data (separate location from site deployment)
    pub_data_dir = SITE_E2E_DIR / "data"
    print("[Cleanup] Removing publication DB data directory...")
    if pub_data_dir.exists():
        shutil.rmtree(pub_data_dir, ignore_errors=True)

    print("[Cleanup] Complete")


# ---------------------------------------------------------------------------
# Utility fixtures for tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def wait_for_replication(local_db_address):
    """
    Return a helper function to wait for replication to catch up.

    Usage in tests:
        def test_something(wait_for_replication):
            # ... insert data into publication ...
            wait_for_replication()
            # ... verify data in local DB ...
    """
    def _wait(timeout=60, max_lag_seconds=5):
        """
        Wait for replication lag to be below threshold.

        Parameters
        ----------
        timeout : int
            Maximum seconds to wait
        max_lag_seconds : float
            Maximum acceptable lag in seconds

        Returns
        -------
        bool
            True if replication caught up, False if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                health = replication.check_replication_health(local_db_address)
                if health and health.get('lag_seconds') is not None:
                    if health['lag_seconds'] < max_lag_seconds:
                        return True
            except Exception:
                pass
            time.sleep(1)

        return False

    return _wait
