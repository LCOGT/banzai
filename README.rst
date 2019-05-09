BANZAI Pipeline
===============

This repo contains the data reduction package for Las Cumbres Observatory (LCO).

BANZAI stands for Beautiful Algorithms to Normalize Zillions of Astronomical Images.

See also `<https://banzai.readthedocs.io>`_ for more information.

Please cite the following DOI if you are using processed LCOGT data.

.. image:: https://zenodo.org/badge/26836413.svg
    :target: https://zenodo.org/badge/latestdoi/26836413
    :alt: Zenodo DOI

.. image:: https://travis-ci.org/LCOGT/banzai.png?branch=master
    :target: https://travis-ci.org/LCOGT/banzai
    :alt: Travis Status

.. image:: https://coveralls.io/repos/github/LCOGT/banzai/badge.svg
    :target: https://coveralls.io/github/LCOGT/banzai
    :alt: Coverage Status

.. image:: https://readthedocs.org/projects/banzai/badge/?version=latest
    :target: http://banzai.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Installation
------------
BANZAI can be installed in the usual way, by running

.. code-block:: bash

    python setup.py install

This will automatically install the dependencies from PyPi, so it is recommended to install
BANZAI in a virtual environment.

Usage
-----
BANZAI can be deployed in two ways, an active pipeline that
processes data as it arrives or a manual pipeline that is run from the command line.

The main requirement to run BANZAI is that the database is setup. BANZAI is database type
agnostic as it uses SQLAlchemy. To create a new database to run BANZAI, run

.. code-block:: python

    from banzai.dbs import create_db
    create_db('.', db_address='sqlite:///banzai.db')

This will create an sqlite3 database file in your current directory called `banzai.db`.

The list of command line entry points are in setup.cfg. You can see more about the parameters
the commands take by adding a `--help` to any command of interest.

If you are not running this at LCO, you will have to add the instrument of interest to your database
by running `banzai_add_instrument` before you can process any data.

By default, BANZAI requires a bad pixel mask. You can create one that BANZAI can use by using the tool
`here <https://github.com/LCOGT/pixel-mask-gen>`_. If the bad pixel mask is in the current directory when you
create the database it will get automatically added. Otherwise run

.. code-block:: python

    from banzai.dbs import populate_calibration_table_with_bpms
    populate_calibration_table_with_bpms('/directory/with/bad/pixel/mask', db_address='sqlite://banzai.db')

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
BANZAI uses the astropy helpers that wrap pytest to run both of it's unit
tests and end-to-end tests.

The end-to-end tests require more setup, so to run only the unit tests locally
run:

.. code-block:: bash

    python setup.py -a "-m 'not e2e'"

The end-to-end tests run on Jenkins at LCO automatically for every pull request.

To run the end-to-end tests locally, the easiest setup uses docker-compose.
In the code directory run:

.. code-block:: bash

    export MINICONDA_VERSION=4.5.11
    export DOCKER_IMG=banzai
    docker build -t $DOCKER_IMG .
    docker-compose up

After all of the containers are up, run

.. code-block:: bash

    docker exec -it banzai-listener /bin/bash
    cd /lco/banzai
    python setup.py test -a "-m e2e"

License
-------
This project is Copyright (c) Las Cumbres Observatory and licensed under the terms of GPLv3. See the LICENSE file for more information.


Support
-------
`Create an issue <https://github.com/LCOGT/banzai/issues>`_

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge