ARG MINICONDA_VERSION
FROM docker.lco.global/docker-miniconda3:${MINICONDA_VERSION}
MAINTAINER Las Cumbres Observatory <webmaster@lco.global>

RUN yum -y install epel-release gcc mariadb-devel \
        && yum -y install fpack \
        && yum -y install "http://packagerepo.lco.gtn/repos/lcogt/7/astrometry.net-0.72-1.lcogt.el7.x86_64.rpm" \
        && yum -y clean all

ENV PATH /opt/astrometry.net/bin:$PATH

RUN conda install -y pip numpy\>=1.13 cython scipy astropy pytest\>=3.6,\<4.0 mock requests ipython coverage\
        && conda install -y -c conda-forge kombu elasticsearch pytest-astropy mysql-connector-python\
        && conda clean -y --all

RUN pip install --no-cache-dir logutils lcogt_logging sqlalchemy\>=1.3.0b1 psycopg2-binary git+https://github.com/kbarbary/sep.git@master

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

COPY --chown=10087:10000 . /lco/banzai

RUN pip install /lco/banzai/ --no-cache-dir

USER archive

ENV HOME /home/archive

WORKDIR /home/archive
