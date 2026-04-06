FROM python:3.13-slim-trixie

# Make a non-privledged user to run the pipeline
RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

RUN apt-get -y update && apt-get -y install gcc procps git && \
        apt-get autoclean && \
        rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV PATH="/lco/banzai/.venv/bin:$PATH"

WORKDIR /lco/banzai

COPY pyproject.toml uv.lock ./

RUN uv sync --no-install-project --no-cache

COPY . .

RUN uv sync --no-cache

RUN cp /lco/banzai/pytest.ini /home/archive/pytest.ini

USER archive

ENV HOME=/home/archive

WORKDIR /home/archive
