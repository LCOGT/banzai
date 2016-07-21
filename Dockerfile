FROM docker.lcogt.net/miniconda3:4.0.5
MAINTAINER Ira W. Snyder <isnyder@lcogt.net>

RUN yum -y install epel-release gcc mariadb-devel \
        && yum -y install fpack \
        && yum -y install "http://nagios.lco.gtn/repos/lcogt/7/astrometry.net-0.64-1.lcogt.el7.x86_64.rpm" \
        && yum -y clean all

ENV PATH /opt/astrometry.net/bin:$PATH

RUN conda install -y pip numpy cython astropy sqlalchemy pytest mock requests ipython \
        && conda clean -y --all

RUN pip install logutils sep mysqlclient lcogt_logging kombu \
        && rm -rf ~/.cache/pip

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

WORKDIR /lco/banzai

COPY . /lco/banzai
RUN python /lco/banzai/setup.py install

USER archive

ENV HOME /home/archive

WORKDIR /home/archive
