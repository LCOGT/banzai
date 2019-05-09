BANZAI Pipeline
===============

This repo contains the data reduction package for Las Cumbres Observatory (LCO).

BANZAI stands for Beautiful Algorithms to Normalize Zillions of Astronomical Images.

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

Usage
-----
BANZAI can be deployed in two ways, an active pipeline that
processes data as it arrives or a manual pipeline that is run from the command line.

The main requirement to run BANZAI is that the database is setup. BANZAI is database type
agnostic as it uses SQLAlchemy. To create a new database to run BANZAI, run

.. code-block:: python

    from banzai.dbs import create_db
    create_db('.', db_address='sqlite:///banzai.db')

This will create an sqlite3 database file in your current directory called ``banzai.db``.

banzai_reduce_individual_frame

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
[API documentation]()
[Create an issue](https://issues.lco.global/)

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge