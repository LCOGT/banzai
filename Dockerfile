FROM continuumio/miniconda3:4.10.3

RUN conda install -y numpy pip scipy astropy pytest mock requests ipython coverage pyyaml\
        && conda install -y -c conda-forge kombu=4.4.0 elasticsearch\<6.0.0,\>=5.0.0 pytest-astropy mysql-connector-python\
        && conda clean -y --all

RUN pip install --no-cache-dir cython logutils lcogt_logging python-dateutil sqlalchemy\>=1.3.0b1 psycopg2-binary celery[redis]==4.3.0 \
        apscheduler ocs-ingester tenacity amqp==2.6.0

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

COPY --chown=10087:10000 . /lco/banzai

RUN apt-get -y update && apt-get -y install gcc && \
        pip install --no-cache-dir git+https://github.com/cmccully/sep.git@deblending /lco/banzai/ && \
        apt-get -y remove gcc && \
        apt-get autoclean && \
        rm -rf /var/lib/apt/lists/*

USER archive

ENV HOME /home/archive

WORKDIR /home/archive
