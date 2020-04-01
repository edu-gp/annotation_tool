from simpletransformers.classification import ClassificationModel

import os
import json
import io
import pickle
import sys
import signal
import traceback
import struct
import redis
import numpy as np
import argparse

from datetime import datetime

import flask

import pandas as pd

import torch
USE_CUDA = torch.cuda.is_available()

prefix = '/opt/ml/'
model_dir = '/opt/program/model'
model_path = '/opt/program/model'

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

def process_argument(args):
    parser = argparse.ArgumentParser(
        description="Peer into the future of a data science project"
    )
    parser.add_argument(
        "--data", type=str, help="valid json string"
    )
    parser.add_argument(
        "--request_id", type=str, help="request_id"
    )
    cargs = parser.parse_args(args)
    return cargs

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
            print(str(datetime.now()) + " Loading model...")
            cls.model = build_model(cls.train_config, model_dir)
            # with open(os.path.join(model_path, 'lr.pkl'), 'rb') as inp:
            #     cls.model = pickle.load(inp)
        print(str(datetime.now()) + " Model loaded...")
        return cls.model

    @classmethod
    def predict(cls, input):
        """For the input, do the predictions and return them.

        Args:
            input (a pandas dataframe): The data on which to do the predictions. There will be
                one prediction per row in the dataframe"""
        # from sklearn.datasets import load_iris
        # X, y = load_iris(return_X_y=True)
        # data = X[:2, :]


        data = input
        clf = cls.get_model()
        print(str(datetime.now()) + " Prediction started...")
        print("input is " + str(data))
        res = clf.predict(data)
        print(str(datetime.now()) + " Prediction completed...")
        return res

if __name__ == "__main__":
    cargs = process_argument(sys.argv[1:])

    data = json.loads(cargs.data)["data"]
    request_id = cargs.request_id
    print("data is " + str(data))
    print("request_id is " + request_id)

    # data=["I like apple", "do you like apple"]
    labels_pred, scores_pred = ScoringService.predict(data)
    predictions = {
        "labels": labels_pred.tolist(),
        "scores": [scores.tolist() for scores in scores_pred]
    }

    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set(request_id, json.dumps(predictions))
    print("Prediction saved.")