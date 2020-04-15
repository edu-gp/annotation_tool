#!/bin/bash

export FLASK_APP=frontend
export FLASK_ENV=development
#flask db init
#flask db migrate
#flask db upgrade
flask run -h 0.0.0.0 --port 5000