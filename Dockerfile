ARG MINICONDA_VERSION
FROM docker.lco.global/docker-miniconda3:${MINICONDA_VERSION}
MAINTAINER Las Cumbres Observatory <webmaster@lco.global>

RUN yum -y install epel-release gcc mariadb-devel \
        && yum -y install fpack \
        && yum -y install "http://packagerepo.lco.gtn/repos/lcogt/7/astrometry.net-0.72-1.lcogt.el7.x86_64.rpm" \
        && yum -y clean all

ENV PATH /opt/astrometry.net/bin:$PATH

RUN conda install -y pip numpy cython scipy astropy pytest==3.5 mock requests ipython coverage\
        && conda install -c openastronomy sep \
        && conda install -c conda-forge kombu elasticsearch pytest-astropy mysql-connector-python\
        && conda clean -y --all

RUN pip install logutils lcogt_logging sqlalchemy\>=1.3.0b1\
        && rm -rf ~/.cache/pip

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

RUN curl -o /usr/local/src/json1.c "https://www.sqlite.org/src/raw/ext/misc/json1.c?name=3f017d2659e531d021d015ec5d69ea0b1c71f2e15bf9768b1e149fcdf6c3e0b1" \
        && gcc -g -fPIC -shared /usr/local/src/json1.c -o /usr/local/lib/json1.so -I /opt/conda/include/

WORKDIR /lco/banzai

COPY . /lco/banzai

RUN python /lco/banzai/setup.py install

USER archive

RUN printf ".load /usr/local/lib/json1" > /home/archive/.sqliterc

ENV HOME /home/archive

WORKDIR /home/archive
