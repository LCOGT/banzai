BANZAI Pipeline
===============

This repo contains the data reduction package for Las Cumbres Observatory (LCO).

BANZAI stands for Beautiful Algorithms to Normalize Zillions of Astronomical Images.

See also `<https://banzai.readthedocs.io>`_ for more information.

Please cite the following DOI if you are using processed LCOGT data.

.. image:: https://zenodo.org/badge/26836413.svg
    :target: https://zenodo.org/badge/latestdoi/26836413
    :alt: Zenodo DOI

We have recently implemented a neural network model to detect cosmic rays in ground based images. For more information
pleas see our paper on arXiv. If possible please also cite
`Xu et al., 2021, arXiv:2106.14922 <https://arxiv.org/abs/2106.14922>`_.

.. image:: https://travis-ci.com/LCOGT/banzai.png?branch=master
    :target: https://travis-ci.com/LCOGT/banzai
    :alt: Test Status

.. image:: https://coveralls.io/repos/github/LCOGT/banzai/badge.svg
    :target: https://coveralls.io/github/LCOGT/banzai
    :alt: Coverage Status

.. image:: https://readthedocs.org/projects/banzai/badge/?version=latest
    :target: http://banzai.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Installation
------------
BANZAI uses `uv <https://docs.astral.sh/uv/>`_ for dependency management. Install uv first if you don't have it:

To run banzai using jupyter notebooks or any other anacode application, you must install banzai from the anaconda terminal using the same procedure.

.. code-block:: bash

    curl -LsSf https://astral.sh/uv/install.sh | sh

Then install BANZAI and all dependencies including CPU-only PyTorch:

.. code-block:: bash

    uv sync

All dependencies are managed automatically. A virtual environment is created in ``.venv`` in the project directory.
Activate it before running any commands:

.. code-block:: bash

    source .venv/bin/activate

Installing from PyPI
~~~~~~~~~~~~~~~~~~~~
When installing BANZAI as a package (rather than from source), pass the pytorch-cpu index to
avoid pulling the full CUDA-bundled PyTorch wheel from PyPI (~2GB):

.. code-block:: bash

    uv pip install lco-banzai --extra-index-url https://download.pytorch.org/whl/cpu

The same flag applies when using ``pip`` instead of ``uv pip``.

If the error `ERROR: Could not build wheels for banzai, which is required to install pyproject.toml-based projects` comes up, on Windows you need to download Microsoft Visual C++ 14.0 or greater.

You may also need to install lcogt_logging, as it is a separate module. This can be done by running `pip3 install lcogt_logging`

Usage
-----

BANZAI has a variety of console entry points:

* `banzai_reduce_individual_frame`: Process a single frame
* `banzai_reduce_directory`: Process all frames in a directory
* `banzai_make_master_calibrations`: Make a master calibration frame by stacking previously processed individual calibration frames.
* `banzai_automate_stack_calibrations`: Start the scheduler that sets when to create master calibration frames
* `banzai_run_realtime_pipeline`: Start the listener to detect and process incoming frames
* `banzai_mark_frame_as_good`: Mark a calibration frame as good in the database
* `banzai_mark_frame_as_bad`: Mark a calibration frame as bad in the database
* `banzai_update_db`: Update the instrument table by querying the ConfigDB
* `banzai_add_instrument`: Add an instrument to the database
* `banzai_add_site`: Add a site to the database
* `banzai_add_super_calibration`: Add a super calibration frame to the database
* `banzai_populate_bpms`: Automatically populate the db with bpms from the archive
* `banzai_create_db`: Initialize a database to be used when running the pipeline
* `banzai_create_local_db`: Initialize a local database for site deployment, copying site/instrument info from a remote calibration database

You can see more about the parameters the commands take by adding a `--help` to any command of interest.

BANZAI can be deployed in two ways: an active pipeline that
processes data as it arrives or a manual pipeline that is run from the command line.

For a runnable example of BANZAI in manual pipeline mode, refer to `this jupyter notebook
<docs/example_reduction.ipynb>`_:

The main requirement to run BANZAI is that the database has been set up. BANZAI is database type
agnostic as it uses SQLAlchemy. To create a new database to run BANZAI, run

.. code-block:: python

    from banzai.dbs import create_db
    create_db('sqlite:///banzai-test.db')

This will create an sqlite3 database file in your current directory called `banzai-test.db`.

To run database migrations (e.g. after upgrading BANZAI), use the Alembic migration files bundled
with the package:

.. code-block:: bash

    DB_ADDRESS=postgresql://user:pass@host/dbname alembic -c $(python -c "import importlib.resources; print(importlib.resources.files('banzai').joinpath('alembic.ini'))") upgrade head

If you are not running this at LCO, you will have to add the instrument of interest to your database
by running `banzai_add_instrument` before you can process any data.

By default, BANZAI requires a bad pixel mask. You can create one that BANZAI can use by using the tool
`here <https://github.com/LCOGT/pixel-mask-gen>`_.
To add a local bpm to the database, run

.. code-block:: bash

    banzai_add_super_calibration path/to/bpm/file --db-address path/to/db

Generally, you have to reduce individual bias frames first by running `banzai_reduce_individual_frame` command.
If the processing went well, you can mark them as good in the database using `banzai_mark_frame_as_good`.
Once you have individually processed bias frames, you can create a master calibration using
`banzai_make_master_calibrations`. This master calibration will then be available for future reductions of
other observation types. Next, similarly reduce individual dark frames and then stack them to
create a master dark frame. Then, the same for skyflats. At this point, you will be able to process
science images using the `banzai_reduce_individual_frame` command.

To run the pipeline in its active mode, you need to setup a task queue and a filename queue.
See the `docker-compose-local.yml` file for details on this setup.

Running Locally
---------------

To run BANZAI as a local pipeline, use `docker-compose-local.yml`. This is the recommended setup
for development and for processing data independently of LCO's site infrastructure.

1. Copy `local-banzai-env.default` to `local-banzai-env` and set your `AUTH_TOKEN` and `DB_ADDRESS`.

2. Start the containers:

.. code-block:: bash

    docker compose -f docker-compose-local.yml --env-file local-banzai-env up -d --build

3. Queue images for processing. Raw files must be in `$HOST_RAW_DIR`:

.. code-block:: bash

    uv run python scripts/queue_images.py $HOST_RAW_DIR

Processed output will be saved in `$HOST_REDUCED_DIR`.

Tests
-----
Unit tests can be run using pytest. The end-to-end tests require more setup, so to run only the unit tests locally run:

.. code-block:: bash

    uv run pytest -m 'not e2e'

The `-m` is short for marker. The following markers are defined if you only want to run a subset of the tests:

* e2e: End-to-end tests. Skip these if you only want to run unit tests.
* master_bias: Only test making a master bias
* master_dark: Only test making a master dark, assumes master bias frame already exists
* master_flat: Only test making a master flat, assumes master bias and dark frames already exist
* science_files: Only test processing science data, assumes master bias, dark, and flat frames already exist.

The end-to-end tests run on Jenkins at LCO automatically for every pull request.

To run the end-to-end tests locally, the easiest setup uses docker-compose.
In the code directory run:

.. code-block:: bash

    export DOCKER_IMG=banzai
    docker build -t $DOCKER_IMG .
    docker-compose up

After all of the containers are up, run

.. code-block:: bash

    docker exec banzai-listener pytest --pyargs banzai.tests "-m e2e"

Site Deployment E2E Tests
~~~~~~~~~~~~~~~~~~~~~~~~~
The site E2E tests validate the full site deployment caching system, including PostgreSQL
logical replication, calibration file caching, and frame reduction. These tests require
Docker and an LCO archive API token.

To run the site E2E tests:

.. code-block:: bash

    # Copy and configure the environment file
    cp banzai/tests/site_e2e/site_e2e.env.template banzai/tests/site_e2e/site_e2e.env
    # Edit site_e2e.env and add your AUTH_TOKEN

    # Run the tests
    pytest -m e2e_site banzai/tests/site_e2e/ -v -s

The following markers can be used to run subsets of the site E2E tests:

* e2e_site: All site deployment tests
* e2e_site_startup: Publication DB and site deployment startup tests
* e2e_site_cache: Cache synchronization tests
* e2e_site_reduction: Frame reduction tests

License
-------
This project is Copyright (c) Las Cumbres Observatory and licensed under the terms of GPLv3. See the LICENSE file for more information.


Support
-------
`Create an issue <https://github.com/LCOGT/banzai/issues>`_

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge
