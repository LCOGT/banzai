# Local Banzai Notes

To run:

``` bash
docker compose -f docker-compose.local.yml --env-file .docker-compose-env up -d --build
```

This requires an env file called .docker-compose-env that should look like this:

``` shellscript
# .docker-compose-env

# Database Address
DB_ADDRESS=

# API Configuration
API_ROOT=https://archive-api.lco.global/
AUTH_TOKEN=""

# Data Paths
HOST_DATA_DIR=./example_data # this maps to /data in the container, and should contain unprocessed data in a subdirectory `raw`
HOST_PROCESSED_DIR=./example_data/output # path where processed data will be saved on the host

# Container Networking
FITS_BROKER=rabbitmq
FITS_BROKER_URL=amqp://rabbitmq:5672
FITS_EXCHANGE=fits_files
TASK_HOST=redis://redis:6379/0

# Celery Configuration
CELERY_TASK_QUEUE_NAME=e2e_task_queue
CELERY_LARGE_TASK_QUEUE_NAME=e2e_large_task_queue

# Worker Configuration
BANZAI_WORKER_LOGLEVEL=debug
OMP_NUM_THREADS=2
OPENTSDB_PYTHON_METRICS_TEST_MODE=1
```

In order to send images to be processed, run:

```bash
python queue_images.py <host_data_dir>/raw
```

The data to be processed should be in the directory `${HOST_DATA_DIR}/raw`. The output will be saved in `./${HOST_PROCESSED_DIR}`.

## Temporary modifications

The following changes remove the database write operations that track whether a file has been processed already.
`dbs.py` in commit_processed_image, line 283-84: added logging and premature return that stops function from running
`utils/realtime_utils.py` in need_to_process_image, line 48-49: added logging and premature return that stops function from running
