FROM python:3.7-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
	 build-essential \
         wget \
         curl \
         git && \
     rm -rf /var/lib/apt/lists/*

WORKDIR /base

COPY requirements.txt .
RUN pip install -r requirements.txt

# Have to add back these two lines. Otherwise the test will fail due to missing dependencies.
RUN pip install torch==1.6.0+cu101 torchvision==0.7.0+cu101 -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install simpletransformers matplotlib seaborn

RUN python -m spacy download en_core_web_sm

# ----------- GOOGLE CLOUD -----------
RUN apt-get update && apt install wget -y
RUN wget -nv \
    https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    mkdir -p /root/tools && \
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
