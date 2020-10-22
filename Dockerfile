FROM python:3.7-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
	 build-essential \
         wget \
         curl \
         git && \
     rm -rf /var/lib/apt/lists/*

WORKDIR /base

ARG TARGET=production
COPY requirements ./requirements
RUN pip install -r requirements/$TARGET.txt

COPY ci ./ci
RUN sh ci/install_deps.sh

# Path configuration
ENV PATH $PATH:/root/tools/google-cloud-sdk/bin
