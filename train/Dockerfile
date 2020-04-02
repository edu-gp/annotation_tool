# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the \"License\");
# you may not use this file except in compliance with the License.\n",
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an \"AS IS\" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# This is the Dockerfile to train models on Google AI Platform
# =============================================================================

# --- Basic Setup ---

# Dockerfile-gpu
FROM nvidia/cuda:10.1-cudnn7-runtime

# Installs necessary dependencies.
RUN apt-get update && apt-get install -y --no-install-recommends \
         wget \
         curl \
         git \
         python3.7 && \
     rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3.7 /usr/bin/python

RUN apt-get update && apt-get install -y python3-distutils

# Installs pip.
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    pip install setuptools && \
    rm get-pip.py

WORKDIR /root

# Installs pytorch and torchvision.
RUN pip install torch==1.4.0 torchvision==0.5.0

# NOTE: In order to install apex _properly_, we need the "devel" images,
# which includes the nvcc command. But that is ~2gb big... I think we're ok
# with a python-only build.
RUN export CUDA_HOME=/usr/local/cuda
RUN git clone https://github.com/NVIDIA/apex
# python-only build... not fully optimized
RUN pip install -v --no-cache-dir apex/
RUN rm -rf apex

RUN pip install simpletransformers matplotlib seaborn

# --- GCP stuff ---

# Installs cloudml-hypertune for hyperparameter tuning.
# It’s not needed if you don’t want to do hyperparameter tuning.
RUN pip install cloudml-hypertune

# Installs google cloud sdk, this is mostly for using gsutil to export model.
RUN wget -nv \
    https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    mkdir /root/tools && \
    tar xvzf google-cloud-sdk.tar.gz -C /root/tools && \
    rm google-cloud-sdk.tar.gz && \
    /root/tools/google-cloud-sdk/install.sh --usage-reporting=false \
        --path-update=false --bash-completion=false \
        --disable-installation-options && \
    rm -rf /root/.config/* && \
    ln -s /root/.config /config && \
    # Remove the backup directory that gcloud creates
    rm -rf /root/tools/google-cloud-sdk/.install/.backup

# Path configuration
ENV PATH $PATH:/root/tools/google-cloud-sdk/bin
# Make sure gsutil will use the default service account
RUN echo '[GoogleCompute]\nservice_account = default' > /etc/boto.cfg

# --- Training Code ---

# Copy in code 
RUN mkdir /root/alchemy
COPY * /root/alchemy/
COPY no_deps /root/alchemy/no_deps

# Sets up the entry point to invoke the trainer.
ENTRYPOINT ["python", "-m", "alchemy.gcp_run"]
