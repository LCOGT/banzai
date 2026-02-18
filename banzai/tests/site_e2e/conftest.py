"""
Pytest fixtures for site E2E tests.

Manages the full site deployment lifecycle:
1. Start and initialize publication database
2. Start site deployment (depends on publication DB being ready)
3. Clean up all resources after tests complete

CRITICAL: Publication must be fully set up before site deployment starts,
as the site's init container will attempt to subscribe to the publication.
"""

import json
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
    """Load KEY=VALUE environment variables from a file. Does not override existing vars."""
    if not env_path.exists():
        return

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip()
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
    """Return the processed_path root where banzai writes calibration files.

    Files are stored in standard banzai hierarchy:
    {cache_dir}/{site}/{camera}/{epoch}/processed/{filename}
    """
    repo_root = SITE_E2E_DIR.parents[2]
    return repo_root / "data" / "calibrations"


@pytest.fixture(scope="session")
def data_dir():
    """Return the path to the test data directory."""
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
    """Return the AUTH_TOKEN from environment. Skips tests if not set."""
    token = os.environ.get("AUTH_TOKEN")
    if not token:
        pytest.skip("AUTH_TOKEN environment variable required for site E2E tests")
    return token


# ---------------------------------------------------------------------------
# Docker Compose helpers
# ---------------------------------------------------------------------------

def run_docker_compose(compose_file, *args, cwd=None, env=None):
    """Run a docker compose command and return the CompletedProcess result."""
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
    """Wait for docker compose services to be healthy. Returns True if healthy within timeout."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = run_docker_compose(compose_file, "ps", "--format", "json", cwd=cwd, env=env)

        if result.returncode == 0 and result.stdout.strip():
            try:
                lines = result.stdout.strip().split('\n')
                all_healthy = True
                found_service = False

                for line in lines:
                    if not line.strip():
                        continue
                    service_info = json.loads(line)
                    svc_name = service_info.get("Service", service_info.get("Name", ""))

                    if service_name and service_name not in svc_name:
                        continue

                    found_service = True
                    state = service_info.get("State", "")
                    health = service_info.get("Health", "")

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
    """Wait for a one-shot service to exit. Returns True if exited with expected_code."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = run_docker_compose(compose_file, "ps", "-a", "--format", "json", cwd=cwd, env=env)

        if result.returncode == 0 and result.stdout.strip():
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
    """Start and initialize the publication database."""
    compose_file = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"

    print("\n[Publication DB] Cleaning up any previous state...")
    run_docker_compose(compose_file, "down", "-v", "--remove-orphans")

    print("[Publication DB] Starting...")
    result = run_docker_compose(compose_file, "up", "-d")
    if result.returncode != 0:
        pytest.fail(f"Failed to start publication DB: {result.stderr}")

    print("[Publication DB] Waiting for healthy...")
    if not wait_for_healthy(compose_file, "postgres", timeout=60):
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
    """Start the full site deployment. Depends on publication_db fixture."""
    repo_root = SITE_E2E_DIR.parents[2]
    compose_file = repo_root / "docker-compose-site.yml"
    env_file = SITE_E2E_DIR / "site_e2e.env"

    if not compose_file.exists():
        pytest.fail(f"docker-compose-site.yml not found at {compose_file}")

    if not env_file.exists():
        pytest.fail(
            f"site_e2e.env not found at {env_file}. "
            f"Copy site_e2e.env.template and fill in required values."
        )

    print("\n[Site Deployment] Cleaning up any previous state...")
    run_docker_compose(
        compose_file,
        "--env-file", str(env_file),
        "down", "-v", "--remove-orphans",
        cwd=repo_root
    )

    for subdir in ["calibrations", "output", "postgres"]:
        d = data_dir / subdir
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)

    (data_dir / "calibrations").mkdir(parents=True, exist_ok=True)
    (data_dir / "output").mkdir(parents=True, exist_ok=True)
    (data_dir / "postgres").mkdir(parents=True, exist_ok=True)

    print("[Site Deployment] Starting...")
    result = run_docker_compose(
        compose_file,
        "--env-file", str(env_file),
        "up", "-d", "--build",
        cwd=repo_root
    )
    if result.returncode != 0:
        pytest.fail(f"Failed to start site deployment: {result.stderr}")

    print("[Site Deployment] Waiting for cache-init to complete...")
    if not wait_for_service_exit(
        compose_file,
        "banzai-cache-init",
        expected_code=0,
        timeout=180,
        cwd=repo_root,
        env={"ENV_FILE": str(env_file)}
    ):
        logs = run_docker_compose(compose_file, "--env-file", str(env_file), "logs", "banzai-cache-init", cwd=repo_root)
        pytest.fail(f"Cache init did not complete successfully. Logs:\n{logs.stdout}\n{logs.stderr}")

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
    """Cleanup fixture that runs after all tests complete."""
    yield

    print("\n[Cleanup] Starting cleanup...")

    repo_root = SITE_E2E_DIR.parents[2]
    site_compose = repo_root / "docker-compose-site.yml"
    pub_compose = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"
    env_file = SITE_E2E_DIR / "site_e2e.env"
    site_id = os.environ.get("SITE_ID", "lsc")

    print("[Cleanup] Dropping subscription...")
    try:
        subscription_name = f"banzai_{site_id}_sub"
        replication.drop_subscription(local_db_address, subscription_name, drop_slot=True)
    except Exception as e:
        print(f"[Cleanup] Warning: Failed to drop subscription: {e}")

    print("[Cleanup] Stopping site deployment...")
    if site_compose.exists():
        run_docker_compose(
            site_compose,
            "--env-file", str(env_file),
            "down", "-v", "--remove-orphans",
            cwd=repo_root
        )

    print("[Cleanup] Stopping publication database...")
    if pub_compose.exists():
        run_docker_compose(pub_compose, "down", "-v", "--remove-orphans")

    print("[Cleanup] Removing data directory contents...")
    if data_dir.exists():
        for item in data_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)

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
    """Return a helper function that waits for replication lag to drop below threshold."""
    def _wait(timeout=60, max_lag_seconds=5):
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


def _wait_for_subscription_active(local_db_address, timeout=60):
    """Wait for the replication subscription to be enabled with an active worker."""
    from sqlalchemy import create_engine, text as sa_text

    engine = create_engine(local_db_address)
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            with engine.connect() as conn:
                result = conn.execute(sa_text("""
                    SELECT s.subname, s.subenabled, ss.pid
                    FROM pg_subscription s
                    LEFT JOIN pg_stat_subscription ss ON s.subname = ss.subname
                    WHERE s.subenabled = true
                """))
                rows = result.fetchall()
                if rows and any(row.pid is not None for row in rows):
                    return True
        except Exception:
            pass
        time.sleep(2)

    return False
