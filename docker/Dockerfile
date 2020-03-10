FROM sjeddie/ed_base

RUN apt-get update
RUN apt-get install -y redis-server vim

RUN pip install redis pytest Flask supervisor Celery seaborn simpletransformers python-dotenv Flask-HTTPAuth
# Some error with latest pillow verison... downgrade pillow
RUN conda install pillow=6.1

RUN conda install -c conda-forge spacy
RUN python -m spacy download en_core_web_sm