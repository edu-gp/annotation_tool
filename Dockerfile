FROM python:3.7-slim as alchemy-base

RUN apt-get update && apt-get install -y --no-install-recommends \
	 build-essential \
         wget \
         curl \
         git \
         xmlsec1 && \
     rm -rf /var/lib/apt/lists/*
# xmlsec1: for SAML

WORKDIR /base

COPY requirements ./requirements
RUN pip install --no-cache -r requirements/base.txt

COPY ci ./ci
RUN sh ci/install_deps.sh

# Path configuration
ENV PATH $PATH:/root/tools/google-cloud-sdk/bin
# This is necessary for the alembic migration to work
ENV PYTHONPATH /app

ENTRYPOINT ["ci/run_server.sh"]


FROM alchemy-base as local
RUN pip install --no-cache -r requirements/local.txt
WORKDIR /app


FROM alchemy-base as alchemy-with-code
WORKDIR /app
COPY . .

FROM alchemy-with-code as production
# https://github.com/bradleyzhou/pun/blob/master/pu/Dockerfile
# Since uwsgi needs C compiler to install, we install the compiler,
#  build uwsgi and other production dependencies, then remove no-longer-required
#  files.

RUN set -ex \
    && buildDeps=' \
        gcc \
        libbz2-dev \
        libc6-dev \
        libgdbm-dev \
        liblzma-dev \
        libncurses-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        libpcre3-dev \
        make \
        tcl-dev \
        tk-dev \
        wget \
        xz-utils \
        zlib1g-dev \
    ' \
    && deps=' \
        libexpat1 \
    ' \
    && apt-get update && apt-get install -y $buildDeps $deps --no-install-recommends  && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache -r requirements/production.txt \
    && apt-get purge -y --auto-remove $buildDeps \
    && find /usr/local -depth \
    \( \
        \( -type d -a -name test -o -name tests \) \
        -o \
        \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
    \) -exec rm -rf '{}' +

FROM production as staging
# It's the same as production. At least for now.

FROM alchemy-with-code as test
RUN pip install --no-cache -r requirements/test.txt
