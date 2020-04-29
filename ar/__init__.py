# Annotation Request Module
import os
from typing import List
from collections import namedtuple

import numpy as np
from sqlalchemy import func

from db import _data_dir
from db._task import _Task
from db.fs import filestore_base_dir, RAW_DATA_DIR
from db.model import get_or_create, Task, fetch_ar_ids_by_task_and_user, \
    AnnotationRequest, Entity, User, AnnotationRequestStatus
from shared.utils import load_jsonl, PrettyDefaultDict
# from db._task import _Task
from inference.base import ITextCatModel
from inference import get_predicted
from inference.random_model import RandomModel

from .data import fetch_all_ar_ids
from .utils import get_ar_id, timeit

Pred = namedtuple('Pred', ['score', 'fname', 'line_number'])


def generate_annotation_requests(dbsession, task_id: str,
                                 max_per_annotator: int, max_per_dp: int):
    '''
    NOTE: This could be super slow, but that's okay for now!
    '''
    task = get_or_create(dbsession=dbsession, model=Task, id=task_id)
    # task = _Task.fetch(task_id)

    print("Get prediction from each model...")
    # TODO better to stream these?

    _examples = []
    _proportions = []

    data_filenames = [os.path.join(filestore_base_dir(), RAW_DATA_DIR,
                                   fname) for fname in
                      task.get_data_filenames()]
    # TODO The Pred returned from get_predictions are based on filename and
    #  line numbers so I have to save a dictionary of entity name here for
    #  the blacklist function, unless we have a better way.
    #  One optimization we can do is to creat this while populate the cache
    #  below since both have to read in all the files.
    entity_per_file_per_line_dict = _build_entity_name_per_file_per_line(
        data_filenames)

    # Random Examples
    _examples.append(
        _get_predictions(data_filenames, [RandomModel()])
    )
    _proportions.append(1)

    # Pattern-driven Examples
    _patterns_model = task.get_pattern_model()
    if _patterns_model is not None:
        _examples.append(
            _get_predictions(data_filenames, [_patterns_model])
        )
        _proportions.append(3)  # [1,3] -> [0.25, 0.75]

    # NLP-driven Examples
    _nlp_model = task.get_active_nlp_model()
    if _nlp_model is not None:
        _examples.append(
            _get_predictions(data_filenames, [_nlp_model])
        )
        _proportions.append(12)  # [1,3,12] -> [0.0625, 0.1875, 0.75]

    print("Shuffling together examples...")
    ordered_examples = _shuffle_together_examples(
        _examples, proportions=_proportions)

    # Blacklist whatever users have labeled already
    # TODO basic logic, given a user_id, task_id, and entity_id, we should
    #  be able to find if there are exisiting requests for this user and
    #  entity under this task. If so, skip those.
    blacklist_fn_db = _build_blacklist_fn_db(task=task)

    print("Assigning to annotators...")
    assignments = _assign_db(dbsession=dbsession,
                             entity_lookup=entity_per_file_per_line_dict,
                             blacklist_fn=blacklist_fn_db,
                             max_per_annotator=max_per_annotator,
                             max_per_dp=max_per_dp)
    # assignments = _assign(ordered_examples, task.annotators,
    #
    #                       blacklist_fn=blacklist_fn,
    #                       max_per_annotator=max_per_annotator,
    #                       max_per_dp=max_per_dp)

    # ---- Populate Cache ----

    # We will need random access for each line in each file.
    __cache_df = {}
    for fname in data_filenames:
        __cache_df[fname] = load_jsonl(fname)

    def get_dp(fname, line_number):
        '''Get Datapoint'''
        return __cache_df[fname].iloc[line_number]

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
        row = get_dp(p.fname, p.line_number)
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

    def get_decorated_example(pred: Pred, entity_lookup: dict):
        idx = __example_idx_lookup[pred]

        ar_id = get_ar_id(pred.fname, pred.line_number)
        res = {
            'ar_id': ar_id,
            'fname': pred.fname,
            'line_number': pred.line_number,
            'score': pred.score,
            'entity_name': entity_lookup[pred.fname][pred.line_number]
        }
        res.update({
            'data': __basic_decor[idx]
        })
        if __pattern_decor is not None:
            res.update({
                'pattern_info': __pattern_decor[idx]
            })
        return res

    # ---- Populate Annotation Requests per Annotator ----

    print("Constructing requests...")
    annotation_requests = {}
    for user, list_of_examples in assignments.items():
        decorated_list_of_examples = [
            get_decorated_example(ex, entity_per_file_per_line_dict)
            for ex in list_of_examples]

        annotation_requests[user] = decorated_list_of_examples

    print("Finished generating annotation requests.")
    return annotation_requests


def _build_entity_name_per_file_per_line(data_filenames):
    entity_per_file_per_line_dict = PrettyDefaultDict(
        lambda: PrettyDefaultDict(str))
    for data_file in data_filenames:
        raw_data = load_jsonl(data_file, to_df=False)
        for i, raw_json in enumerate(raw_data):
            entity_per_file_per_line_dict[data_file][i] = raw_json["meta"][
                "domain"]
    return entity_per_file_per_line_dict


def _build_blacklist_fn_db(task: Task):
    UserEntityTaskTuple = namedtuple('UserEntityTaskTuple', ['user',
                                                             'entity', 'task'])
    _lookup_db = dict()

    def blacklist_fn(dbsession, pred: Pred,
                     annotator: str,
                     entity_lookup: dict):
        entity_name = entity_lookup[pred.fname][pred.line_number]
        lookup_key = UserEntityTaskTuple(annotator, entity_name, task.id)
        if lookup_key not in _lookup_db:
            res = dbsession.query(func.count(AnnotationRequest.id)). \
                join(Entity). \
                join(User). \
                filter(Entity.name == entity_name,
                       User.username == annotator,
                       AnnotationRequest.task_id == task.id,
                       AnnotationRequest.status ==
                       AnnotationRequestStatus.Complete).one()[0]
            _lookup_db[lookup_key] = res > 0
        return _lookup_db[lookup_key]

    return blacklist_fn


def _build_blacklist_fn(task: _Task):
    _lookup = {
        user: set(fetch_all_ar_ids(task.task_id, user))
        for user in task.annotators
    }

    # blacklist_fn = lambda thing, user : False
    def blacklist_fn(pred: Pred, annotator: str):
        ar_id = get_ar_id(pred.fname, pred.line_number)
        return ar_id in _lookup[annotator]

    return blacklist_fn


def _get_predictions(data_filenames: List[str], models: List[ITextCatModel],
                     cache=True):
    '''
    Return the aggregated score from all models for all lines in each data_filenames

    Return Pred namedtuple (score, fname, line_number)
    '''
    result = []

    for fname in data_filenames:
        preds = []

        for model in models:
            res = get_predicted(fname, model, cache=cache)
            preds.append([x['score'] for x in res])

        if len(preds) > 0:
            # Get total score from all models.
            total_scores = np.sum(preds, axis=0)

            for line_number, score in enumerate(total_scores):
                result.append(Pred(score, fname, line_number))

    return result


def _assign_db(dbsession, entity_lookup: dict,
               datapoints: List, annotators: List,
               max_per_annotator: int, max_per_dp: int,
               blacklist_fn=None):

    # TODO data point is a Pred(score, fname, line_number)
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

    # TODO data point is a Pred(score, fname, line_number)
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

            if (pred.fname, pred.line_number) not in seen:
                # We've found an element from ls that can be added to res!
                # TODO keep track of which list the pred comes from; for easier debugging
                seen.add((pred.fname, pred.line_number))
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
