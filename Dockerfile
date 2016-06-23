FROM continuumio/miniconda3:4.0.5
MAINTAINER Austin Riba <ariba@lcogt.net>
WORKDIR /lco/banzai

RUN echo "deb http://http.us.debian.org/debian testing main non-free contrib" >> /etc/apt/sources.list && \
    echo "deb-src http://http.us.debian.org/debian testing main non-free contrib" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y astrometry.net build-essential libmysqlclient-dev mysql-client && \
    conda install numpy cython astropy sqlalchemy pytest mock requests ipython && \
    pip install logutils sep mysqlclient lcogt_logging kombu

RUN /usr/sbin/groupadd -g 10000 "domainusers" && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive

COPY . /lco/banzai
RUN python /lco/banzai/setup.py install

USER archive

ENV HOME /home/archive

WORKDIR /home/archive
