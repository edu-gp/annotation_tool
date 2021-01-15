#!/usr/bin/env bash

PARAMS=()
if [ -z $FLASK_ENV ]; then export FLASK_ENV=production; fi
if [ -z $FLASK_RUN_HOST ]; then export FLASK_RUN_HOST=0.0.0.0; fi
if [ -z $FLASK_RUN_PORT ]; then export FLASK_RUN_PORT=5000; fi
if [ -z $SCRIPT_NAME ]; then export SCRIPT_NAME=/; fi

while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "This is a script to launch alchemy server in development and production."
      echo "USGAGE:"
      echo "  $0 [OPTIONS] (admin_server|annotation_server)"
      echo "  "
      echo "OPTIONS:"
      echo "  --flask-env (development|production|...)"
      echo "                        \t Overrides FLASK_ENV"
      echo "  --flask-host ip       \t Overrides FLASK_RUN_HOST"
      echo "  --flask-port port,    \t Overrides FLASK_RUN_PORT"
      echo "    -p port"
      echo "  --database databse_uri\t Overrides ALCHEMY_DATABASE_URI"
      exit 0
      ;;
    --flask-env)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        export FLASK_ENV=$2
        shift 2
      else
        echo "Error: Missing argument for $1" >&2
        exit 1
      fi
      ;;
    --flask-host)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        export FLASK_RUN_HOST=$2
        shift 2
      else
        echo "Error: Missing argument for $1" >&2
        exit 1
      fi
      ;;
    -p|--flask-port)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        export FLASK_RUN_PORT=$2
        shift 2
      else
        echo "Error: Missing argument for $1" >&2
        exit 1
      fi
      ;;
    --database)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        export ALCHEMY_DATABASE_URI=$2
        shift 2
      else
        echo "Error: Missing argument for $1" >&2
        exit 1
      fi
      ;;
    *) # preserve positional arguments
      PARAMS+=( "$1" )
      shift
      ;;
  esac
done
if [ ${#PARAMS[@]} -eq 0 ]; then
  echo "Required parameter app directory not provided"
  exit 1
fi

export FLASK_APP=alchemy/${PARAMS[0]}
unset 'PARAMS[0]'

# TODO: maybe do a backup before migrations if alembic is not doing transactions?
export DB_URL_FOR_MIGRATION=$ALCHEMY_DATABASE_URI
alembic upgrade head

python alchemy/scripts/check_filesystem_health.py

if [ $FLASK_ENV = 'development' ] || [ $FLASK_ENV = 'test' ]; then
  flask run ${PARAMS[*]}
else
  echo Run the production server

  uwsgi --socket :$FLASK_RUN_PORT  \
        --master \
        --vacuum \
        --harakiri 20 \
        --max-requests 2000 \
        --manage-script-name \
        --mount $SCRIPT_NAME=alchemy/wsgi.py \
        --callable app \
        ${PARAMS[*]}
fi
