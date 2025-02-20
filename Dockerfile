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

COPY environment.yaml .
RUN conda init
SHELL ["/bin/bash", "--login", "-c"]

# RUN conda activate base && conda config --set remote_read_timeout_secs 900 && conda env update -f environment.yaml --solver=libmamba && conda clean --all

# COPY --chown=10087:10000 . /lco/banzai

# RUN conda activate base && pip install --no-cache-dir /lco/banzai/ 

# # Don't ask me why but something about the install breaks sqlite but this fixes it
# RUN conda activate base && conda install libsqlite --force-reinstall -y

# RUN cp /lco/banzai/pytest.ini /home/archive/pytest.ini
