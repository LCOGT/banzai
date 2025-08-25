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
BANZAI can be installed using pip, by running from the top-level directory containing `setup.py`.

Note that `pip>=19.3.1` is required to build and install BANZAI.

.. code-block:: bash

    pip install .

This will automatically install the dependencies from PyPi, so it is recommended to install
BANZAI in a virtual environment.

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

You can see more about the parameters the commands take by adding a `--help` to any command of interest.

BANZAI can be deployed in two ways: an active pipeline that
processes data as it arrives or a manual pipeline that is run from the command line.

For a runnable example of BANZAI in manual pipeline mode, refer to `this jupyter notebook
<docs/manual_reduction.ipynb>`_:

The main requirement to run BANZAI is that the database has been set up. BANZAI is database type
agnostic as it uses SQLAlchemy. To create a new database to run BANZAI, run

.. code-block:: python

    from banzai.dbs import create_db
    create_db('sqlite:///banzai-test.db')

This will create an sqlite3 database file in your current directory called `banzai-test.db`.

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
`banzai_stack_calibrations`. This master calibration will then be available for future reductions of
other observation types. Next, similarly reduce individual dark frames and then stack them to
create a master dark frame. Then, the same for skyflats. At this point, you will be able to process
science images using the `banzai_reduce_individual_frame` command.

To run the pipeline in its active mode, you need to setup a task queue and a filename queue.
See the `docker-compose.yml` file for details on this setup.

Tests
-----
Unit tests can be run using pytest. The end-to-end tests require more setup, so to run only the unit tests locally run:

.. code-block:: bash

    pytest -m 'not e2e'

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

License
-------
This project is Copyright (c) Las Cumbres Observatory and licensed under the terms of GPLv3. See the LICENSE file for more information.


Support
-------
`Create an issue <https://github.com/LCOGT/banzai/issues>`_

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge
