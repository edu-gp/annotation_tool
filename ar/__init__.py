# Annotation Request Module

import os
from typing import List

import numpy as np
from shared.utils import load_jsonl, save_jsonl, mkf
from db.task import Task, DIR_ANNOTATORS
from inference.base import ITextCatModel
from inference.pattern_model import PatternModel
from inference import get_predicted_cached

def generate_annotation_requests(data_filenames:List[str], models:List[ITextCatModel], n=100):
    '''
    NOTE: This is super slow, but that's okay for now!

    This uses some simple Active Learning to rank the files
    '''
    fnames = []
    line_numbers = []
    scores = []

    for fname in data_filenames:
        preds = []

        for model in models:
            res = get_predicted_cached(fname, model)
            preds.append( [x['score'] for x in res] )

        # Get total score from all models.
        total_scores = np.sum(preds, axis=0)

        N = len(total_scores)
        fnames += [fname] * N
        line_numbers += list(range(N))
        scores += list(total_scores)

    # TODO check we don't already have labeled data in the db.
    top_n_idx = np.argsort(scores)[::-1][:n]

    # TODO insert some random ones in the end

    # Random access within a file is almost as bad as reading them all into mem.
    __cache_df = {}
    for fname in data_filenames:
        __cache_df[fname] = load_jsonl(fname)

    # NOTE any kind of decoration should also be done here. (Could be a long-running process.)

    # Precompute extra info about patterns, if any pattern model exists
    pattern_info = []
    pattern_model = None
    for model in models:
        if isinstance(model, PatternModel):
            pattern_model = model
            break
    if pattern_model is not None:
        # Gather all the text in the annotations to show (small list)
        text_list = []
        for idx in top_n_idx:
            row = __cache_df[fnames[idx]].iloc[[line_numbers[idx]]].iloc[0]
            text_list.append(row['text'])
        # Do a "fancy" inference
        resp = pattern_model.predict(text_list, fancy=True)
        pattern_info = resp

    # Construct the annotation requests
    annotation_requests = []
    for i, idx in enumerate(top_n_idx):
        row = __cache_df[fnames[idx]].iloc[[line_numbers[idx]]].iloc[0]

        req = {
            # (fname, line_number) is used as the unique id of a datapoint.
            'fname': fnames[idx],
            'line_number': line_numbers[idx],

            'score': scores[idx],
            'data': {
                'text': row['text'],
                'meta': row['meta']
            }
        }

        # Extra information for prettier labeling
        if len(pattern_info) > 0:
            req.update({
                'pattern_info': pattern_info[i]
            })

        annotation_requests.append(req)

    return annotation_requests

def generate_annotation_requests_for_user(task_id, user_id, n=100):
    task = Task.fetch(task_id)

    annotation_requests = generate_annotation_requests(task.data_filenames, task.models, n)

    # TODO this should be Task's responsibility
    # TODO save to Redis?
    fname = f'{user_id}.jsonl'
    path = [task.get_dir(), DIR_ANNOTATORS, fname]
    fname = os.path.join(*path)
    mkf(*path)

    save_jsonl(fname, annotation_requests)

    return fname
