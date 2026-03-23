"""Pytest fixtures for site E2E tests."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from sqlalchemy import create_engine, text as sa_text
from kombu import Connection, Queue

import pytest

from banzai.cache import replication
from banzai.logs import get_logger
from banzai.tests.site_e2e.utils import populate_publication

logger = get_logger()


SITE_E2E_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SITE_E2E_DIR.parents[2]


def load_env_file(env_path):
    """Load KEY=VALUE environment variables from a file. Does not override existing vars."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, _, value = line.partition('=')
            os.environ.setdefault(key.strip(), value.strip())


load_env_file(SITE_E2E_DIR / "site_e2e.env")

# Configuration constants (from env vars with defaults)
PUBLICATION_DB_ADDRESS = os.environ.get(
    "PUBLICATION_DB_ADDRESS",
    "postgresql+psycopg://banzai:banzai_test@localhost:5433/banzai_test"
)
LOCAL_DB_ADDRESS = os.environ.get(
    "LOCAL_DB_ADDRESS",
    "postgresql+psycopg://banzai@localhost:5442/banzai_local"
)
ARCHIVE_API_URL = os.environ.get("API_ROOT", "https://archive-api.lco.global/")
DATA_DIR = REPO_ROOT / "data"
CACHE_DIR = DATA_DIR / "calibrations"
SITE_COMPOSE_FILE = REPO_ROOT / "docker-compose-site.yml"
SITE_ENV_FILE = SITE_E2E_DIR / "site_e2e.env"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def poll_until(predicate, timeout, interval=5):
    """Poll predicate() until truthy or timeout. Returns last result."""
    start = time.time()
    result = None
    while time.time() - start < timeout:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return result


def publish_to_queue(queue_name, body, broker_url='amqp://localhost:5672'):
    """Publish a JSON message to a RabbitMQ queue."""
    with Connection(broker_url) as conn:
        queue = Queue(queue_name, channel=conn.channel())
        queue.declare()
        with conn.Producer() as producer:
            producer.publish(body, routing_key=queue_name, serializer='json')


def run_docker_compose(compose_file, *args, cwd=None, env=None):
    """Run a docker compose command and return the CompletedProcess result."""
    cmd = ["docker", "compose", "-f", str(compose_file)] + list(args)
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(
        cmd, cwd=cwd, env=merged_env, capture_output=True, text=True, check=False
    )


def run_site_compose(*args):
    """Run docker compose for the site deployment."""
    return run_docker_compose(
        SITE_COMPOSE_FILE, "--env-file", str(SITE_ENV_FILE), *args, cwd=REPO_ROOT
    )


def wait_for_healthy(compose_file, service_name=None, timeout=60, cwd=None, env=None):
    """Wait for docker compose services to be healthy."""
    def check():
        result = run_docker_compose(compose_file, "ps", "--format", "json", cwd=cwd, env=env)
        if result.returncode != 0 or not result.stdout.strip():
            return False
        try:
            found = False
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                info = json.loads(line)
                name = info.get("Service", info.get("Name", ""))
                if service_name and service_name not in name:
                    continue
                found = True
                if info.get("State") != "running":
                    return False
                health = info.get("Health", "")
                if health and health != "healthy":
                    return False
            return found
        except (json.JSONDecodeError, KeyError):
            return False

    return poll_until(check, timeout, interval=2)


def wait_for_service_exit(compose_file, service_name, expected_code=0, timeout=120, cwd=None, env=None):
    """Wait for a one-shot service to exit with expected_code."""
    def check():
        result = run_docker_compose(compose_file, "ps", "-a", "--format", "json", cwd=cwd, env=env)
        if result.returncode != 0 or not result.stdout.strip():
            return False
        try:
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                info = json.loads(line)
                name = info.get("Service", info.get("Name", ""))
                if service_name in name and info.get("State") == "exited":
                    return info.get("ExitCode", -1) == expected_code
        except (json.JSONDecodeError, KeyError):
            pass
        return False

    return poll_until(check, timeout, interval=2)


def drop_subscription_keep_slot(db_address, subscription_name):
    """Drop a local subscription without dropping the replication slot on the publisher.

    This simulates the scenario where the local DB is wiped but the publisher
    retains the slot (e.g., container restart with a fresh volume).
    """
    engine = create_engine(db_address)
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(sa_text(f"ALTER SUBSCRIPTION {subscription_name} DISABLE"))
        conn.execute(sa_text(f"ALTER SUBSCRIPTION {subscription_name} SET (slot_name = NONE)"))
        conn.execute(sa_text(f"DROP SUBSCRIPTION {subscription_name}"))
    engine.dispose()


def wait_for_subscription_active(timeout=60):
    """Wait for the replication subscription to be enabled with an active worker."""

    engine = create_engine(LOCAL_DB_ADDRESS)

    def check():
        try:
            with engine.connect() as conn:
                result = conn.execute(sa_text("""
                    SELECT 1 FROM pg_subscription s
                    JOIN pg_stat_subscription ss ON s.subname = ss.subname
                    WHERE s.subenabled = true AND ss.pid IS NOT NULL
                    LIMIT 1
                """))
                return result.fetchone() is not None
        except Exception:
            return False

    return poll_until(check, timeout, interval=2)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def auth_token():
    """Return the AUTH_TOKEN from environment. Skips tests if not set."""
    token = os.environ.get("AUTH_TOKEN")
    if not token:
        pytest.skip("AUTH_TOKEN environment variable required for site E2E tests")
    return token


@pytest.fixture(scope="session")
def publication_db():
    """Start and initialize the publication database."""
    compose_file = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"

    logger.info("\n[Publication DB] Cleaning up any previous state...")
    run_docker_compose(compose_file, "down", "-v", "--remove-orphans")

    logger.info("[Publication DB] Starting...")
    result = run_docker_compose(compose_file, "up", "-d")
    if result.returncode != 0:
        pytest.fail(f"Failed to start publication DB: {result.stderr}")

    logger.info("[Publication DB] Waiting for healthy...")
    if not wait_for_healthy(compose_file, "postgres", timeout=60):
        logs = run_docker_compose(compose_file, "logs")
        pytest.fail(f"Publication DB did not become healthy. Logs:\n{logs.stdout}\n{logs.stderr}")

    logger.info("[Publication DB] Inserting initial data...")
    try:
        populate_publication.insert_initial_data(PUBLICATION_DB_ADDRESS)
    except Exception as e:
        pytest.fail(f"Failed to populate publication DB: {e}")

    logger.info("[Publication DB] Ready")
    yield PUBLICATION_DB_ADDRESS


@pytest.fixture(scope="session")
def site_deployment(publication_db):
    """Start the full site deployment. Depends on publication_db fixture."""
    if not SITE_COMPOSE_FILE.exists():
        pytest.fail(f"docker-compose-site.yml not found at {SITE_COMPOSE_FILE}")
    if not SITE_ENV_FILE.exists():
        pytest.fail("site_e2e.env not found. Copy site_e2e.env.template and fill in required values.")

    logger.info("\n[Site Deployment] Cleaning up any previous state...")
    run_site_compose("down", "-v", "--remove-orphans")

    for subdir in ["raw", "calibrations", "output", "postgres"]:
        d = DATA_DIR / subdir
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)

    logger.info("[Site Deployment] Starting...")
    result = run_site_compose("up", "-d", "--build")
    if result.returncode != 0:
        pytest.fail(f"Failed to start site deployment: {result.stderr}")

    logger.info("[Site Deployment] Waiting for cache-init to complete...")
    if not wait_for_service_exit(
        SITE_COMPOSE_FILE, "banzai-cache-init", expected_code=0, timeout=180,
        cwd=REPO_ROOT, env={"ENV_FILE": str(SITE_ENV_FILE)}
    ):
        logs = run_site_compose("logs", "banzai-cache-init")
        pytest.fail(f"Cache init did not complete. Logs:\n{logs.stdout}\n{logs.stderr}")

    logger.info("[Site Deployment] Waiting for services to be healthy...")
    if not wait_for_healthy(
        SITE_COMPOSE_FILE, timeout=120,
        cwd=REPO_ROOT, env={"ENV_FILE": str(SITE_ENV_FILE)}
    ):
        logs = run_site_compose("logs")
        pytest.fail(f"Site services not healthy. Logs:\n{logs.stdout}\n{logs.stderr}")

    logger.info("[Site Deployment] Ready")
    yield


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup fixture that runs after all tests complete."""
    yield

    logger.info("\n[Cleanup] Starting cleanup...")
    pub_compose = SITE_E2E_DIR / "publication_db" / "docker-compose.yml"
    site_id = os.environ.get("SITE_ID", "lsc")

    logger.info("[Cleanup] Dropping subscription...")
    try:
        replication.drop_subscription(LOCAL_DB_ADDRESS, f"banzai_{site_id}_sub")
    except Exception as e:
        logger.warning(f"[Cleanup] Failed to drop subscription: {e}")

    logger.info("[Cleanup] Stopping site deployment...")
    if SITE_COMPOSE_FILE.exists():
        run_site_compose("down", "-v", "--remove-orphans")

    logger.info("[Cleanup] Stopping publication database...")
    if pub_compose.exists():
        run_docker_compose(pub_compose, "down", "-v", "--remove-orphans")

    logger.info("[Cleanup] Removing data directory contents...")
    if DATA_DIR.exists():
        for item in DATA_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)

    logger.info("[Cleanup] Complete")
