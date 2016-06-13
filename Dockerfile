FROM continuumio/anaconda3:4.0.0
MAINTAINER Austin Riba <ariba@lcogt.net>
WORKDIR /lco/banzai

RUN echo "deb http://http.us.debian.org/debian testing main non-free contrib" >> /etc/apt/sources.list && \
    echo "deb-src http://http.us.debian.org/debian testing main non-free contrib" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y astrometry.net build-essential libmysqlclient-dev mysql-client

COPY . /lco/banzai
RUN python /lco/banzai/setup.py install
