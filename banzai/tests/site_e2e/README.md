# Site Deployment E2E Tests

End-to-end tests that verify the complete site deployment caching system works correctly.

## What These Tests Verify

The banzai site deployment uses PostgreSQL logical replication to cache calibration files locally at observatory sites. This allows sites to continue processing data even during network outages.

These tests validate the full system by:

1. **Publication Database Setup** - A PostgreSQL database with logical replication enabled, simulating the central AWS database that publishes calibration metadata.

2. **Replication Subscription** - The site deployment subscribes to the publication database and receives calibration records automatically.

3. **File Caching** - The download worker detects new calibrations in the local database and downloads the actual FITS files from the archive API.

4. **Version Management** - When older calibrations are added, the download worker correctly keeps only the top 2 most recent per configuration (instrument, type, mode, binning, filter).

5. **Frame Reduction** - A raw science frame is processed using the cached calibrations.

## Prerequisites

- Docker and Docker Compose installed
- Access to the LCO archive API (requires an auth token)

## Configuration

1. Copy the environment template:
   ```bash
   cp banzai/tests/site_e2e/site_e2e.env.template banzai/tests/site_e2e/site_e2e.env
   ```

2. Edit `site_e2e.env` and fill in:

   - Your archive API token:
     ```bash
     AUTH_TOKEN=your-lco-archive-api-token
     ```
   - Absolute paths for `HOST_RAW_DIR`, `HOST_CALS_DIR`, and `HOST_REDUCED_DIR`. Relative paths will cause tests to fail fast at collection time, because `docker-compose-site.yml` mounts these as `${HOST_*_DIR}:${HOST_*_DIR}` (same path inside and outside the container). Point them at `<your-checkout>/data/raw`, `/calibrations`, and `/output`. (PostgreSQL data lives in the `site-pgdata` Docker-managed named volume.)

   Other values have working defaults.

## Running the Tests

Run all site E2E tests:
```bash
pytest banzai/tests/site_e2e/ -v -s
```

The tests will automatically:
- Start the publication database container
- Populate it with test calibration metadata
- Start the full site deployment (PostgreSQL, Redis, RabbitMQ, workers)
- Run the test suite
- Clean up all containers and data when finished

## Test Markers

You can run specific test phases:

```bash
# Startup tests only (publication DB, site deployment, replication)
pytest -m e2e_site_startup banzai/tests/site_e2e/ -v

# Cache tests only (replication sync, file downloads)
pytest -m e2e_site_cache banzai/tests/site_e2e/ -v

# Reduction tests only (frame processing)
pytest -m e2e_site_reduction banzai/tests/site_e2e/ -v
```

## Watching Logs

While tests run, you can monitor service logs in separate terminals. The e2e
fixture brings the site stack up under compose project `banzai-e2e` with the
container prefix `e2e-banzai-`:

```bash
docker logs -f e2e-banzai-download-worker  # Watch file downloads
docker logs -f e2e-banzai-cache-init       # Watch replication setup
docker logs -f e2e-banzai-worker           # Watch frame processing
```

## Running Alongside the local or site compose files

The e2e stack is namespaced (compose project `banzai-e2e`, container prefix
`e2e-banzai-`, explicit `e2e_*` queue/exchange names, postgres on host port
5443) so it can run concurrently with a developer site/local stack on the same
machine:

- Container names don't collide (`e2e-banzai-worker` vs `banzai-worker`).
- Queues don't collide (`e2e_reduction_task_queue` vs `reduction_task_queue`),
  even though both stacks share the same `banzai-redis` / `banzai-rabbitmq`.
- Postgres host ports don't collide (5443 for e2e, 5442 for site-up).
- `docker compose down` from the e2e fixture is project-scoped — it cannot
  tear down a docker-compose-site stack (project `banzai`).

The only manual collision risk is the host data directories (`HOST_RAW_DIR`,
`HOST_CALS_DIR`, `HOST_REDUCED_DIR`). The e2e default is `<repo>/data/...`;
production site-up uses `/mnt/data/...`. Keep them on different paths.

## Troubleshooting

**Tests skip with "AUTH_TOKEN environment variable required"**
- Ensure you copied the template and set your token in `site_e2e.env`

**Publication DB fails to start**
- Check if port 5433 is already in use
- Run `docker compose -f banzai/tests/site_e2e/publication_db/docker-compose.yml logs` for details

**Cache init fails**
- The publication must exist before site deployment starts
- Check that the publication_db container is healthy before site containers start

**Files not downloading**
- Verify your AUTH_TOKEN is valid and has archive access
- Check download worker logs: `docker logs e2e-banzai-download-worker`
