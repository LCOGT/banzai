"""
Pytest fixtures for smart stacking integration tests.

These fixtures manage Docker infrastructure (PostgreSQL + Redis) and
provide test data for integration testing against real services.
"""

import os
import subprocess
import time
from pathlib import Path

import pytest
import redis

from banzai import dbs


INTEGRATION_DIR = Path(__file__).parent.resolve()


def load_env_file(env_path):
    """Load KEY=VALUE pairs from env file, don't override existing vars."""
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


ENV_FILE = INTEGRATION_DIR / "smart_stacking_integration.env"
load_env_file(ENV_FILE)


def run_docker_compose(*args):
    compose_file = INTEGRATION_DIR / "docker-compose.yml"
    cmd = ["docker", "compose", "-f", str(compose_file)] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def wait_for_healthy(service_name=None, timeout=60):
    """Wait for docker compose services to be healthy."""
    import json
    compose_file = INTEGRATION_DIR / "docker-compose.yml"
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "ps", "--format", "json"],
            capture_output=True, text=True, check=False
        )
        if result.returncode == 0 and result.stdout.strip():
            try:
                lines = result.stdout.strip().split('\n')
                all_healthy = True
                found = False
                for line in lines:
                    if not line.strip():
                        continue
                    info = json.loads(line)
                    svc = info.get("Service", info.get("Name", ""))
                    if service_name and service_name not in svc:
                        continue
                    found = True
                    if info.get("State") != "running":
                        all_healthy = False
                    elif info.get("Health", "") and info["Health"] != "healthy":
                        all_healthy = False
                if found and all_healthy:
                    return True
            except (json.JSONDecodeError, KeyError):
                pass
        time.sleep(2)
    return False


@pytest.fixture(scope="session")
def db_address():
    return os.environ.get("DB_ADDRESS", "postgresql://banzai@localhost:5450/smartstack_test")


@pytest.fixture(scope="session")
def redis_url():
    return os.environ.get("REDIS_URL", "redis://localhost:6390/0")


@pytest.fixture(scope="session")
def infrastructure(db_address, redis_url):
    """Start Docker services, create DB schema, yield, tear down."""
    print("\n[Integration] Cleaning up previous state...")
    run_docker_compose("down", "-v", "--remove-orphans")

    print("[Integration] Starting services...")
    result = run_docker_compose("up", "-d")
    if result.returncode != 0:
        pytest.fail(f"Failed to start services: {result.stderr}")

    print("[Integration] Waiting for healthy...")
    if not wait_for_healthy(timeout=60):
        logs = run_docker_compose("logs")
        pytest.fail(f"Services not healthy. Logs:\n{logs.stdout}\n{logs.stderr}")

    print("[Integration] Creating DB schema...")
    dbs.create_db(db_address)

    print("[Integration] Ready")
    yield

    print("\n[Integration] Tearing down...")
    run_docker_compose("down", "-v", "--remove-orphans")


@pytest.fixture(scope="session")
def redis_client(infrastructure, redis_url):
    """Return a Redis client connected to the test Redis."""
    client = redis.from_url(redis_url)
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_redis(redis_client):
    """Clean stacking Redis keys before each test."""
    yield
    for key in redis_client.keys('stack:notify:*'):
        redis_client.delete(key)


