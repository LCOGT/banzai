# Running Banzai Locally

## Set env variables

First copy `.site-banzai-env.default` to a new file `.site-banzai-env` and provide values for the databases,
site, and api auth token.

Traditionally, banzai relies on a single database to store information about sites, instruments, calibration files, and
processed data. This is the default behavior if the `CAL_DB_ADDRESS` is not supplied, or if it is equal to `DB_ADDRESS`.

The use case for LCO's banzai setup at site assumes that calibration files are processed and managed elsewhere, which
allows us to use a remote database to source any super calibrations needed for local reductions (via `CAL_DB_ADDRESS`),
with all other db activity happening on a local sqlite database (via `DB_ADDRESS`)

Note that these db addresses are defined here relative to the docker container. Since we map the host's `$HOST_DATA_DIR`
to the container's `/data`, the value for `DB_ADDRESS` should be something like `sqlite:////data/<db_name>`, and should
exist in the host directory `$HOST_DATA_DIR/<db_name>`.

## Create the local database

If using a separate calibration database, we need to copy the site/instrument information into our local db for
reference. This can be done with the following:

``` bash
banzai_create_local_db --site $SITE_ID --db-address $LOCAL_DB_ADDRESS --cal-db-address $CAL_DB_ADDRESS
```

Note that the `$LOCAL_DB_ADDRESS` is different than `$DB_ADDRESS` defined in the env file above. The local address should
use a path relative to the host directory rather than the container. For example, if the `$HOST_DATA_DIR` is
`site_banzai`, the local db address might look like `sqlite:///site_banzai/<db_name>`.

## Start the banzai containers

The container configuration is defined in docker-compose-site.yml. Run it with the env file specified:

``` bash
docker compose -f docker-compose-site.yml --env-file .site-banzai-env up -d --build
```

## Processing images

Images for the site defined in $SITE_ID can be processed by notifying the listener queue. The raw files must be stored
in `$HOST_DATA_DIR`. The following example demonstrates a command to process images stored in the `raw/` subdirectory
using the `queue_images.py` helper script.

```bash
python queue_images.py <host_data_dir>/raw/
```

The output will be saved in `./${HOST_PROCESSED_DIR}`.
