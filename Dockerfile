FROM python:3.7-slim as alchemy-base

RUN apt-get update && apt-get install -y --no-install-recommends \
	 build-essential \
         wget \
         curl \
         git && \
     rm -rf /var/lib/apt/lists/*

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
CMD ["admin_server"]

FROM alchemy-base as local
RUN pip install -r requirements/local.txt
WORKDIR /app


FROM alchemy-base as alchemy-with-code
WORKDIR /app
COPY . .

FROM alchemy-with-code as production
RUN pip install -r requirements/production.txt

FROM alchemy-with-code as staging
RUN pip install -r requirements/production.txt

FROM alchemy-with-code as test
RUN pip install -r requirements/test.txt
