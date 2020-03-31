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

import flask

import pandas as pd

import torch
USE_CUDA = torch.cuda.is_available()

prefix = '/opt/ml/'
model_dir = '/opt/program/model'

def build_model(config, model_dir=None, weight=None):
    """
    Inputs:
        config: train_config, see train_celery.py
        model_dir: a trained model's output dir, None if model has not been trained yet
        weight: class weights
    """
    contents = os.listdir(model_dir)
    print(contents)

    return ClassificationModel(
        'roberta', model_dir or 'roberta-base',
        use_cuda=USE_CUDA,
        args={
            # https://github.com/ThilinaRajapakse/simpletransformers/#sliding-window-for-long-sequences
            'sliding_window': config.get('sliding_window', False),

            'reprocess_input_data': True,
            'overwrite_output_dir': True,

            'use_cached_eval_features': False,
            'no_cache': True,

            'num_train_epochs': config['num_train_epochs'],
            'weight': weight,

            # TODO I don't need checkpoints yet - disable this to save disk space
            'save_eval_checkpoints': False,
            'save_model_every_epoch': False,
            'save_steps': 999999,

            # Bug in the library, need to specify it here and in the .train_model kwargs
            'output_dir': config.get('model_output_dir'),

            # Note: 512 requires 16g of GPU mem. You can try 256 for 8g.
            'max_seq_length': config.get('max_seq_length', 512),
        }
    )

# A singleton for holding the model. This simply loads the model and holds it.
# It has a predict function that does a prediction based on the model and the input data.

class ScoringService(object):
    model = None                # Where we keep the model when it's loaded
    train_config = {
        'model_output_dir': '/tmp',
        'num_train_epochs': 5,
        'sliding_window': True,
        'max_seq_length': 512,
    }

    @classmethod
    def get_model(cls):
        """Get the model object for this instance, loading it if it's not already loaded."""
        if cls.model == None:
            cls.model = build_model(cls.train_config, model_dir)
        return cls.model

    @classmethod
    def predict(cls, input):
        """For the input, do the predictions and return them.

        Args:
            input (a pandas dataframe): The data on which to do the predictions. There will be
                one prediction per row in the dataframe"""
        clf = cls.get_model()
        return clf.predict(input)

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
    data = None

    # Convert from CSV to pandas
    if flask.request.content_type == 'application/json':
        data = flask.request.data.decode('utf-8')
        print(data)
        print(type(data))
        s = io.StringIO(data).getvalue()
        data = json.loads(s)["data"]
        print(data)
        print(type(data))
    else:
        print('This predictor only supports JSON data')
        return flask.Response(response='This predictor only supports JSON data', status=415, mimetype='text/plain')

    # print('Invoked with {} records'.format(data.shape[0]))

    # Do the prediction
    predictions = ScoringService.predict(data)

    # Convert from numpy back to CSV
    out = io.StringIO()
    pd.DataFrame({'results':predictions}).to_csv(out, header=False, index=False)
    result = out.getvalue()

    return flask.Response(response=result, status=200, mimetype='text/csv')
