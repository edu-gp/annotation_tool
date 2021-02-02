#!/usr/bin/env sh

# Parse the arguments
INSTALL_GCP=1
INSTALL_SPACY=1
SPACY_MODEL="en_core_web_sm"
while test $# -gt 0; do
  case "$1" in
    -h|--help)
    echo "This script is used to install all the dependencies that"
    echo "Alchemy needs to work with."
    echo ""
    echo -e "--no-gcp\tDoes not install GCP tools"
    echo -e "--no-spacy\tDoes not download the language model"
    echo -e "--spacy-model [model_name]\tSpecifies the spacy model name to download"
    exit 0
    ;;
    --no-gcp)
      INSTALL_GCP=0
      shift
      ;;
    --no-spacy)
      INSTALL_SPACY=0
      shift
      ;;
    --spacy-model)
      shift
      if test $# -eq 0; then
        echo "No model name specified"
        exit 1
      fi
      if test $INSTALL_SPACY -eq 0; then
        echo "Cannot download the model without installing spacy"
        exit 1
      fi
      SPACY_MODEL=$1
      shift
      ;;
    *)
      break
      ;;
  esac
done


echo Installing extra python dependencies
pip install --no-cache torch==1.6.0+cu101 torchvision==0.7.0+cu101 -f https://download.pytorch.org/whl/torch_stable.html
pip install --no-cache transformers==3.5.1 simpletransformers==0.49.7 matplotlib seaborn

if test $INSTALL_SPACY -eq 1; then
  echo Downloading the spacy english language model
  python -m spacy download $SPACY_MODEL
fi

# ----------- GOOGLE CLOUD -----------
if test $INSTALL_GCP -eq 1; then
  apt-get update && apt install wget -y
  mkdir -p /root/tools
  wget -nv https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    tar xvzf google-cloud-sdk.tar.gz -C /root/tools && \
    rm google-cloud-sdk.tar.gz
  /root/tools/google-cloud-sdk/install.sh \
      --usage-reporting=false \
      --path-update=false \
      --bash-completion=false \
      --disable-installation-options
  rm -rf /root/.config/*
  ln -s /root/.config /config
  # Remove the backup directory that gcloud creates
  rm -rf /root/tools/google-cloud-sdk/.install/.backup
fi