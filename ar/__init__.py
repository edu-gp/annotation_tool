# Annotation Request Module
import logging
import os
from typing import List, Dict
from collections import namedtuple

import numpy as np
from sqlalchemy import func

from db.fs import filestore_base_dir, RAW_DATA_DIR
from db.model import Task, User, ClassificationAnnotation, \
    AnnotationValue, LabelPatterns, get_latest_model_for_label
from shared.utils import load_jsonl
from inference.base import ITextCatModel
from inference import get_predicted
from inference.random_model import RandomModel
from inference.pattern_model import PatternModel
from inference.nlp_model import (
    NLPModel, NLPModelTopResults, NLPModelBottomResults
)

Example = namedtuple(
    'Example', ['score', 'entity_type', 'entity', 'label', 'fname', 'line_number'])
LookupKey = namedtuple(
    'LookupKey', ['entity_type', 'entity', 'label', 'user'])


def get_pattern_model_for_label(dbsession, label):
    from db._task import _convert_to_spacy_patterns

    label_patterns = dbsession.query(
        LabelPatterns).filter_by(label=label).first()
    if label_patterns:
        patterns = label_patterns.get_positive_patterns()
        if len(patterns) > 0:
            patterns = _convert_to_spacy_patterns(patterns)
            model = PatternModel(patterns)
            return model

    return None


def get_nlp_models_for_label(dbsession, label):
    """
    Create active learning models for this label based on a trained NLP model.
    """
    highest_entropy_model = None
    top_prob_model = None
    bottom_prob_model = None

    latest_model = get_latest_model_for_label(dbsession=dbsession, label=label)

    if latest_model and latest_model.is_ready():
        highest_entropy_model = NLPModel(dbsession, latest_model.id)
        top_prob_model = NLPModelTopResults(dbsession, latest_model.id)
        bottom_prob_model = NLPModelBottomResults(dbsession, latest_model.id)

    return highest_entropy_model, top_prob_model, bottom_prob_model


def get_ranked_examples_for_label(dbession, label, data_filenames) -> List[Example]:
    """Get the ranking for each datapoint in data_filenames for this label.
    (We're also passing in the task, but in the future I hope to remove this
    dependency)
    A lower ranking means higher desire to be labeled.
    """
    logging.info(f"Get prediction from label={label}")

    examples: List[List[Example]] = []
    proportions: List[int] = []

    # TODO do not hard-code
    from db.model import EntityTypeEnum
    entity_type = EntityTypeEnum.COMPANY

    def get_examples_for_model(model: ITextCatModel):
        return _get_examples(data_filenames, model, entity_type, label)

    # Random Examples
    examples.append(get_examples_for_model(RandomModel()))
    proportions.append(1)
    logging.info("Prediction from random model finished")

    # Pattern-driven Examples
    _patterns_model = get_pattern_model_for_label(dbession, label)
    if _patterns_model:
        examples.append(get_examples_for_model(_patterns_model))
        proportions.append(3)  # [1,3] -> [0.25, 0.75]
        logging.info("Prediction from pattern model finished")

    # NLP-driven Examples
    highest_entropy_model, top_prob_model, bottom_prob_model = \
        get_nlp_models_for_label(dbession, label)

    if highest_entropy_model:
        examples.append(get_examples_for_model(highest_entropy_model))
        proportions.append(12)  # [1,3,12] -> [0.0625, 0.1875, 0.75]
        logging.info("Prediction from highest entropy nlp model finished")

    if top_prob_model:
        examples.append(get_examples_for_model(top_prob_model))
        proportions.append(6)  # [1,3,12,6] -> [0.05, 0.14, 0.55, 0.27]
        logging.info("Prediction from top prob nlp model finished")

    if bottom_prob_model:
        examples.append(get_examples_for_model(bottom_prob_model))
        proportions.append(6)  # [1,3,12,6,6] -> [0.04, 0.11, 0.43, 0.21, 0.21]
        logging.info("Prediction from bottom prob nlp model finished")

    ranked_examples = _shuffle_together_examples(
        examples, proportions=proportions)
    logging.info("Shuffle together examples finished")

    return ranked_examples


def consolidate_ranked_examples_per_label(
        ranked_examples_per_label: List[List[Example]]) -> List[Example]:
    """
    Inputs:
        ranked_examples_per_label: Each element in this list, List[Example], 
            represents the ranked examples according to a label.
    """
    # A naive way to consolidate is just to round-robin preferences across all
    # the lists. Eg. 1st choice of label A, 1st choice of label B, ... then
    # 2nd choice of label A, 2nd choice of label B, ...

    # Alternative approach is we aggregate preferences, sort by that.
    # However a potential issue is that if all the top preferences are not
    # shared, but the mediocre preferences are, then we might end up selecting
    # mediocre examples that won't benefit any label in particular.

    # Will stick with the naive way for now.

    # Note: Our current algo assumes all labels are weighted equally - in the
    # future we can add something to favor certain labels over others.

    seen = set()
    res = []
    i = 0
    max_len = max([len(ls) for ls in ranked_examples_per_label])
    while i < max_len:
        for ls in ranked_examples_per_label:
            if i < len(ls):
                example = ls[i]
                # print(example)
                if example.entity not in seen:
                    seen.add(example.entity)
                    res.append(example)
        i += 1

    return res


def generate_annotation_requests(dbsession, task_id: int,
                                 max_per_annotator: int, max_per_dp: int):
    '''
    NOTE: This could be super slow, but that's okay for now!
    '''
    task = dbsession.query(Task).filter_by(id=task_id).one_or_none()
    assert task is not None, f"Task is missing task_id={task_id}"

    # TODO Restrict each task to use only 1 file?
    data_filenames = [os.path.join(filestore_base_dir(), RAW_DATA_DIR, fname)
                      for fname in task.get_data_filenames()]

    ranked_examples_per_label = []
    for label in task.get_labels():
        _res = get_ranked_examples_for_label(dbsession, label, data_filenames)
        ranked_examples_per_label.append(_res)

    ranked_examples = consolidate_ranked_examples_per_label(
        ranked_examples_per_label)

    # Blacklist whatever users have labeled already
    # basic logic, given a user_id, task_id, and entity_id, we should
    # be able to find if there are exisiting requests for this user and
    # entity under this task. If so, skip those.
    logging.info("Constructing blacklisting criteria...")
    lookup = _build_blacklist_lookup(dbsession, task.get_labels())
    blacklist_fn = _build_blacklist_fn(lookup)

    logging.info("Assigning to annotators...")
    assignments = _assign(ranked_examples,
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
    __pattern_decor_for_all_labels = []
    labels = task.get_labels()
    for label in labels:
        pattern_model_for_label = get_pattern_model_for_label(
            dbsession=dbsession, label=label)
        print(pattern_model_for_label)
        if pattern_model_for_label:
            __pattern_decor_for_label = pattern_model_for_label.predict(
                __text_list, fancy=True
            )
            __pattern_decor_for_all_labels.append(__pattern_decor_for_label)

    __pattern_decor = _merge_pattern_decor_for_all_labels_recursive(
        pattern_decor_for_all_labels=__pattern_decor_for_all_labels,
        low=0,
        high=len(__pattern_decor_for_all_labels) - 1
    )

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


def _merge_pattern_decor_for_all_labels_recursive(
        pattern_decor_for_all_labels: List[List], low: int, high: int):
    """Merge pattern decors from different labels into one pattern decor
    using a recursive merge like the logic in merge sort.

    As discussed, we decided to show all the pattern matches from different
    labels together on the frontend with the same color for now. This may
    cause some confusion as there may be overlap of matching positions from
    different labels. For example, one match could be (5, 7) from label A
    and the other could be (6, 9) from label B. In this case, (5, 9) will be
    highlighted since the frontend will highlight both (5, 7) and (6,
    9) separately. Since we use the same color, it appears as we are
    highlighting (5, 9) as one match.

    :param pattern_decor_for_all_labels: A list of pattern decor lists
    :param low: low index of the merge range
    :param high: high index of the merge range
    :return: A merged pattern decor list
    """
    if len(pattern_decor_for_all_labels) == 0:
        return None
    if low == high:
        return pattern_decor_for_all_labels[low]
    if low > high:
        return None

    mid = low + (high - low) // 2
    left_merged = _merge_pattern_decor_for_all_labels_recursive(
        pattern_decor_for_all_labels, low, mid
    )
    right_merged = _merge_pattern_decor_for_all_labels_recursive(
        pattern_decor_for_all_labels, mid + 1, high
    )

    return _merge_two_pattern_decors(left_merged, right_merged)


def _merge_two_pattern_decors(pattern_decor1: List, pattern_decor2: List):
    res = pattern_decor1.copy()
    for i, item in enumerate(pattern_decor2):
        # Tokens for the same entity are the same so need to merge them.

        res[i]['matches'].extend(item['matches'])

        # In case we need the scores later on, they are merged into a list
        if not isinstance(res[i]['score'], list):
            res[i]['score'] = [res[i]['score']]

        if isinstance(item['score'], list):
            res[i]['score'].extend(item['score'])
        else:
            res[i]['score'].append(item['score'])

    # Dedup the merged matching positions.
    for item in res:
        item['matches'] = sorted(list(set(item['matches'])))

    return res


def _build_blacklist_lookup(dbsession, labels: List[str] = []) -> set:
    existing_annotations_for_labels = \
        dbsession.query(
            ClassificationAnnotation.entity_type,
            ClassificationAnnotation.entity,
            ClassificationAnnotation.label,
            User.username
        ). \
        join(User). \
        filter(
            ClassificationAnnotation.value != AnnotationValue.NOT_ANNOTATED,
            ClassificationAnnotation.label.in_(labels)
        ). \
        distinct().all()
    lookup = set([
        LookupKey(item[0], item[1], item[2], item[3])
        for item in existing_annotations_for_labels
    ])
    return lookup


def _get_decorated_example(pred: Example,
                           pattern_decor: List,
                           basic_decor: List,
                           example_idx_lookup: Dict) -> Dict:
    idx = example_idx_lookup[pred]

    res = {
        'fname': pred.fname,
        'line_number': pred.line_number,
        'score': pred.score,
        'entity': pred.entity
    }
    res.update({
        'data': basic_decor[idx]
    })
    if pattern_decor is not None:
        res.update({
            'pattern_info': pattern_decor[idx]
        })
    return res


def _build_blacklist_fn(lookup_set: set):

    def blacklist_fn(e: Example, user: str):
        return LookupKey(e.entity_type, e.entity, e.label, user) in lookup_set

    return blacklist_fn


def _get_examples(data_filenames: List[str], model: ITextCatModel,
                  entity_type: str, label: str,
                  cache=True) -> List[Example]:
    '''Construct an Example based on the prediction of `model` on each of the
    datasets.'''
    examples = []

    for fname in data_filenames:
        res = get_predicted(fname, model, cache=cache)
        # TODO remove dependency on (fname,line_number)
        for line_number, row in enumerate(res):
            examples.append(Example(score=row['score'],
                                    entity_type=entity_type,
                                    # TODO remove dependency on meta.domain
                                    entity=row['meta']['domain'],
                                    label=label,
                                    fname=fname,
                                    line_number=line_number))

    return examples


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

    return per_anno_queue


def _shuffle_together_examples(list_of_examples: List[List[Example]],
                               proportions: List[float]):
    '''
    Randomly shuffle together lists of Example, such that at any length,
    the proportion of elements from each list is approximately `proportions`.
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
            if pred.entity is None:
                continue

            if pred.entity not in seen:
                # We've found an element from ls that can be added to res!
                # TODO keep track of which list the pred comes from; for easier debugging
                seen.add(pred.entity)
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
