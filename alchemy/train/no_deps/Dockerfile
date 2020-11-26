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
	    build-essential wget curl git python3.7 libpython3.7-dev && \
    ln -s /usr/bin/python3.7 /usr/bin/python && \
    apt-get install -y python3-distutils && \
    rm -rf /var/lib/apt/lists/* && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    pip install --no-cache setuptools && \
    rm get-pip.py && \
    pip install --no-cache simpletransformers matplotlib seaborn envparse==0.2.0 \
        torch==1.6.0+cu101 torchvision==0.7.0+cu101 \
        -f https://download.pytorch.org/whl/torch_stable.html



WORKDIR /root

# --- GCP stuff ---

# Installs cloudml-hypertune for hyperparameter tuning.
# It’s not needed if you don’t want to do hyperparameter tuning.
RUN pip install --no-cache cloudml-hypertune && \
    wget -nv https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
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
COPY . /root/alchemy

# Sets up the entry point to invoke the trainer.
ENTRYPOINT ["python", "-m", "alchemy.gcp_run"]
