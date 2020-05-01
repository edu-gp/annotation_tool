# Annotation Request Module
import logging
import os
from typing import List, Dict
from collections import namedtuple

import numpy as np
from sqlalchemy import func

from db.fs import filestore_base_dir, RAW_DATA_DIR
from db.model import get_or_create, Task, \
    AnnotationRequest, Entity, User, AnnotationRequestStatus
from shared.utils import load_jsonl
from inference.base import ITextCatModel
from inference import get_predicted
from inference.random_model import RandomModel

from .data import fetch_all_ar_ids
from .utils import get_ar_id, timeit

Pred = namedtuple('Pred', ['score', 'entity_name', 'fname',  'line_number'])
UserEntityTaskTuple = namedtuple('UserEntityTaskTuple', ['user', 'entity'])


def generate_annotation_requests(dbsession, task_id: int,
                                 max_per_annotator: int, max_per_dp: int):
    '''
    NOTE: This could be super slow, but that's okay for now!
    '''
    task = get_or_create(dbsession=dbsession, model=Task, id=task_id)

    logging.info("Get prediction from each model...")

    _examples = []
    _proportions = []

    data_filenames = [os.path.join(filestore_base_dir(), RAW_DATA_DIR, fname)
                      for fname in task.get_data_filenames()]

    # Random Examples
    _examples.append(
        _get_predictions(data_filenames, [RandomModel()])
    )
    _proportions.append(1)
    logging.info("Prediction from random model finished...")

    # Pattern-driven Examples
    _patterns_model = task.get_pattern_model()
    if _patterns_model is not None:
        _examples.append(
            _get_predictions(data_filenames, [_patterns_model])
        )
        _proportions.append(3)  # [1,3] -> [0.25, 0.75]
        logging.info("Prediction from pattern model finished...")

    # NLP-driven Examples
    _nlp_model = task.get_active_nlp_model()
    if _nlp_model is not None:
        _examples.append(
            _get_predictions(data_filenames, [_nlp_model])
        )
        _proportions.append(12)  # [1,3,12] -> [0.0625, 0.1875, 0.75]
        logging.info("Prediction from nlp model finished...")

    logging.error("Shuffling together examples...")
    ordered_examples = _shuffle_together_examples(
        _examples, proportions=_proportions)

    # Blacklist whatever users have labeled already
    # basic logic, given a user_id, task_id, and entity_id, we should
    # be able to find if there are exisiting requests for this user and
    # entity under this task. If so, skip those.
    logging.info("Constructing blacklisting criteria...")
    num_of_complete_requests_by_entity_and_user_within_task = \
        dbsession.query(
            func.count(AnnotationRequest.id),
            Entity.name,
            User.username
        ). \
        join(Entity). \
        join(User). \
        filter(AnnotationRequest.task_id == task.id,
               AnnotationRequest.status ==
               AnnotationRequestStatus.Complete). \
        group_by(Entity.name, User.username).all()
    lookup_dict = {
        (item[1], item[2]): item[0]
        for item in num_of_complete_requests_by_entity_and_user_within_task
    }
    blacklist_fn = _build_blacklist_fn(lookup_dict=lookup_dict)

    logging.info("Assigning to annotators...")
    assignments = _assign(ordered_examples,
                          task.get_annotators(),
                          blacklist_fn=blacklist_fn,
                          max_per_annotator=max_per_annotator,
                          max_per_dp=max_per_dp)
    logging.info("Assigning to annotators finished...")

    # ---- Populate Cache ----

    # We will need random access for each line in each file.
    __cache_df = {}
    for fname in data_filenames:
        __cache_df[fname] = load_jsonl(fname)

    # It's faster to get decorated examples in batch.
    __examples = set()
    for annotator, list_of_examples in assignments.items():
        for pred in list_of_examples:
            __examples.add(pred)
    __examples = list(__examples)

    # Basic decorations
    # Also generate a __text_list to be used for other decorators.
    __basic_decor = []
    __text_list = []
    for p in __examples:
        row = __cache_df[p.fname].iloc[p.line_number]
        __basic_decor.append({
            'text': row['text'],
            'meta': row['meta']
        })
        __text_list.append(row['text'])

    # Pattern decorations
    if task.get_pattern_model():
        __pattern_decor = task.get_pattern_model().predict(__text_list,
                                                           fancy=True)
    else:
        __pattern_decor = None

    # Build up a dict for random access
    __example_idx_lookup = dict(zip(__examples, range(len(__examples))))

    # ---- Populate Annotation Requests per Annotator ----

    logging.info("Constructing requests...")
    annotation_requests = {}
    for user, list_of_examples in assignments.items():
        decorated_list_of_examples = [
            _get_decorated_example(ex,
                                   __pattern_decor,
                                   __basic_decor,
                                   __example_idx_lookup)
            for ex in list_of_examples]

        annotation_requests[user] = decorated_list_of_examples

    logging.info("Finished generating annotation requests.")
    return annotation_requests


def _get_decorated_example(pred: Pred,
                           pattern_decor: List,
                           basic_decor: List,
                           example_idx_lookup: Dict) -> Dict:
    idx = example_idx_lookup[pred]

    res = {
        'fname': pred.fname,
        'line_number': pred.line_number,
        'score': pred.score,
        'entity_name': pred.entity_name
    }
    res.update({
        'data': basic_decor[idx]
    })
    if pattern_decor is not None:
        res.update({
            'pattern_info': pattern_decor[idx]
        })
    return res


def _build_blacklist_fn(lookup_dict: dict):

    def blacklist_fn(pred: Pred, annotator: str):
        entity_name = pred.entity_name
        lookup_key = UserEntityTaskTuple(annotator, entity_name)
        if lookup_key not in lookup_dict:
            lookup_dict[lookup_key] = 0
        return lookup_dict[lookup_key] > 0

    return blacklist_fn


def _get_predictions(data_filenames: List[str],
                     models: List[ITextCatModel],
                     cache=True) -> List[Pred]:
    '''
    Return the aggregated score from all models for all lines in each
    data_filenames

    Return Pred namedtuple (score, entity_name, fname, line_number)
    '''
    result = []

    for fname in data_filenames:
        preds = []
        metas = []

        # TODO how can we obtain just one meta from res? If we have 3
        #  models, we get 3 identical copy of entities right now.
        for model in models:
            res = get_predicted(fname, model, cache=cache)
            preds.append([x['score'] for x in res])
            metas.append([x['meta'] for x in res])

        if len(preds) > 0:
            # Get total score from all models.
            total_scores = np.sum(preds, axis=0)
            print(total_scores)

            for line_number, score in enumerate(total_scores):
                result.append(Pred(score=score,
                                   entity_name=metas[0][line_number][
                                          'domain'],
                                   fname=fname,
                                   line_number=line_number))

    return result


def _assign(datapoints: List, annotators: List,
            max_per_annotator: int, max_per_dp: int,
            blacklist_fn=None):
    '''
    Args:
        datapoints: A list of data points to assign to each annotator.
        annotators: A list of annotators.
        blacklist_fn: fn(datapoint, annotator) that returns if an annotator has previously annotated this thing.
        max_per_annotator: How many datapoints should each annotator strive to have.
        max_per_dp: How many annotators should each datapoint strive to have.
    Returns:
        Assignment in the form of:
        {
            annotator:  list of datapoints
            ...
        }
    '''

    def is_valid(anno, dp):
        if anno in per_dp_queue[dp]:
            # This datapoint already assigned to this annotator
            return False
        if blacklist_fn(dp, anno):
            # User specified function to not allow this
            return False
        if len(per_anno_queue[anno]) >= max_per_annotator:
            # This annotator has too many datapoints to label already
            return False
        return True

    def get_next_anno(dp):
        put_back = []
        ret_anno = None

        while not anno_q.empty():
            item = anno_q.get()
            anno = item[1]

            if is_valid(anno, dp):
                ret_anno = anno
                break
            else:
                put_back.append(item)

        for item in put_back:
            anno_q.put(item)

        return ret_anno

    if blacklist_fn is None:
        def blacklist_fn(datapoint, annotator): return False

    from queue import PriorityQueue
    from collections import defaultdict

    anno_q = PriorityQueue()
    for anno in annotators:
        # Each annotator starts off with 0 datapoint assigned
        anno_q.put((0, anno))

    per_dp_queue = defaultdict(list)
    per_anno_queue = defaultdict(list)

    for dp in datapoints:
        for i in range(max_per_dp):
            anno = get_next_anno(dp)
            # print(dp, i, anno)
            if anno is None:
                # No more possible users to annotate this
                break
            else:
                per_dp_queue[dp].append(anno)
                per_anno_queue[anno].append(dp)
                anno_q.put((len(per_anno_queue[anno]), anno))

    # TODO insert random jobs to trade explore/exploit?

    return per_anno_queue


def _shuffle_together_examples(list_of_examples: List[List[Pred]],
                               proportions: List[float]):
    '''
    Randomly shuffle together lists of Pred, such that at any length, the proportion of elements
    from each list is approximately `proportions`.
    '''

    # Sort each list from top to bottom
    list_of_examples = [
        sorted(x, key=lambda pred: pred.score, reverse=True) for x in
        list_of_examples]

    res = []
    seen = set()
    # Current index into each list
    ls_idx = [0 for _ in range(len(list_of_examples))]

    # Normalize proportions
    proportions = np.array(proportions)
    proportions = proportions / np.sum(proportions)

    # Note this is "worst case" O(total_n = total number of items in all lists)
    # Where "worst case" means if we have a bug in the loop.
    # We can actually expect this to run in O(number of unique filename:linenum pairs)

    total_n = sum([len(x) for x in list_of_examples])

    for _ in range(total_n):
        which_list = np.argmax(np.random.multinomial(
            1, proportions, size=1), axis=1)[0]
        ls = list_of_examples[which_list]

        # Try our best to add 1 element from ls to res.
        while ls_idx[which_list] < len(ls):
            pred = ls[ls_idx[which_list]]
            ls_idx[which_list] += 1
            # TODO could name and/or domain be null? How would that affect
            #  the tuple as a key? Can we just skip it?
            if pred.entity_name is None:
                continue

            if pred.entity_name not in seen:
                # We've found an element from ls that can be added to res!
                # TODO keep track of which list the pred comes from; for easier debugging
                seen.add(pred.entity_name)
                res.append(pred)
                break

        if ls_idx[which_list] == len(ls):
            # We've exhausted the selected list. Time to reshuffle the proportions.
            proportions[which_list] = 0

            if np.isclose(np.sum(proportions), 0):
                # We're done! Nothing more to sample.
                break
            else:
                # Normalize
                proportions = proportions / np.sum(proportions)

    # assert np.isclose(np.sum(proportions), 0) # Should be always true, but not needed...
    return res
