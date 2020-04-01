# This is the file that implements a flask server to do inferences. It's the file that you will modify to
# implement the scoring for your own algorithm.

from __future__ import print_function
from simpletransformers.classification import ClassificationModel

import os
import json
import io
import pickle
import sys
import signal
import traceback
import redis

from datetime import datetime

import flask

import uuid

import pandas as pd

import torch
USE_CUDA = torch.cuda.is_available()

prefix = '/opt/ml/'
model_dir = '/opt/program/model'
model_path = '/opt/program/model'
r = redis.Redis(host='localhost', port=6379, db=0)

# The flask app for serving predictions
app = flask.Flask(__name__)

@app.route('/ping', methods=['GET'])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    health = ScoringService.get_model() is not None  # You can insert a health check here

    status = 200 if health else 404
    return flask.Response(response='\n', status=status, mimetype='application/json')

@app.route('/invocations', methods=['POST'])
def transformation():
    """Do an inference on a single batch of data. In this sample server, we take data as CSV, convert
    it to a pandas data frame for internal use and then convert the predictions back to CSV (which really
    just means one prediction per line, since there's a single column.
    """
    # create_a_dummy_sklearn_model()
    request_id = str(uuid.uuid4())

    data = None
    
    # Convert from CSV to pandas
    if flask.request.content_type == 'application/json':
        data = flask.request.data.decode('utf-8')
        print(data)
        print(type(data))
        s = io.StringIO(data).getvalue()
        # data = json.loads(s)["data"]
        # print(data)
        # print(type(data))
    else:
        print('This predictor only supports JSON data')
        return flask.Response(response='This predictor only supports JSON data', status=415, mimetype='text/plain')

    # print('Invoked with {} records'.format(data.shape[0]))

    # Do the prediction
    # predictions = ScoringService.predict(data)
    command_array = [
        'python', 'predict_data.py', '--data', s, 
        '--request_id', request_id
    ]

    import subprocess
    subprocess.run(
        command_array
    )
    predictions = r.get(request_id)
    print(predictions)
    # stdout_value = proc.communicate()[0].decode('utf-8')
    # print('stdout:', repr(stdout_value))
    # predictions = stdout_value
    # print(predictions)

    # Convert from numpy back to CSV
    result = json.loads(predictions)
    # pd.DataFrame({'results':predictions}).to_csv(out, header=False, index=False)
    # result = out.getvalue()

    return flask.Response(response=result, status=200, mimetype='application/json')
