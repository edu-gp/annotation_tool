#!/usr/bin/env sh

# TODO: add commandline options to override the variables
export FLASK_APP=alchemy/$1
if [ -z $FLASK_ENV ]; then export FLASK_ENV=production; fi
if [ -z $FLASK_RUN_HOST ]; then export FLASK_RUN_HOST=0.0.0.0; fi
if [ -z $FLASK_RUN_PORT ]; then export FLASK_RUN_PORT=5000; fi

# TODO: maybe do a backup before migrations if alembic is not doing transactions?
export DB_URL_FOR_MIGRATION=$ALCHEMY_DATABASE_URI
alembic upgrade head

echo Run the development server
# TODO: run it over uwsgi, flask run is for development only
flask run