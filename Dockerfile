FROM continuumio/miniconda3:23.5.2-0

#  In principle I could remove the gcc to shrink the image, but pytorch is already so large it doesn't make much difference
RUN apt-get -y update && apt-get -y install gcc && \
        apt-get autoclean && \
        rm -rf /var/lib/apt/lists/*

RUN mkdir /home/archive && /usr/sbin/groupadd -g 10000 "domainusers" \
        && /usr/sbin/useradd -g 10000 -d /home/archive -M -N -u 10087 archive \
        && chown -R archive:domainusers /home/archive

USER archive

ENV HOME /home/archive

WORKDIR /home/archive

COPY environment.yaml .

RUN  . /opt/conda/etc/profile.d/conda.sh && conda config --set remote_read_timeout_secs 900 && conda env create -p /home/archive/envs/banzai -f environment.yaml --solver=libmamba

COPY --chown=10087:10000 . /lco/banzai

ENV PATH /home/archive/envs/banzai/bin:$PATH

RUN /home/archive/envs/banzai/bin/pip install --no-cache-dir /lco/banzai/ 
