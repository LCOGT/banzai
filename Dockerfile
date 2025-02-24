FROM python:3.12-slim

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

RUN pip install poetry --no-cache

RUN apt-get -y update && apt-get -y install gcc && \
        apt-get autoclean && \
        rm -rf /var/lib/apt/lists/*

USER archive

ENV HOME=/home/archive

WORKDIR /home/archive

COPY --chown=10087:10000 pyproject.toml poetry.lock /lco/banzai/

RUN poetry install --directory=/lco/banzai -E cpu --no-root --no-cache

COPY --chown=10087:10000 . /lco/banzai

RUN poetry install --directory /lco/banzai -E cpu --no-cache

RUN cp /lco/banzai/pytest.ini /home/archive/pytest.ini
