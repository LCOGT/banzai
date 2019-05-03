ARG MINICONDA_VERSION
FROM docker.lco.global/docker-miniconda3:${MINICONDA_VERSION}
MAINTAINER Las Cumbres Observatory <webmaster@lco.global>

RUN yum -y install epel-release gcc mariadb-devel \
        && yum -y install fpack \
        && yum -y clean all

RUN conda install -y numpy==1.15.4 pip scipy astropy pytest mock requests ipython coverage pyyaml\
        && conda install -y -c conda-forge kombu elasticsearch\<6.0.0,\>=5.0.0 pytest-astropy mysql-connector-python\
        && conda clean -y --all

RUN pip install --no-cache-dir git+https://github.com/cython/cython.git@master logutils lcogt_logging sqlalchemy\>=1.3.0b1 psycopg2-binary celery[redis] \
        apscheduler git+https://github.com/kbarbary/sep.git@master

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

COPY --chown=10087:10000 . /lco/banzai

RUN pip install --global-option=build_ext /lco/banzai/ --no-cache-dir

USER archive

ENV HOME /home/archive

WORKDIR /home/archive
