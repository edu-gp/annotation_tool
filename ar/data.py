import os
import shutil
import itertools
import re
import glob
import time
from collections import defaultdict, Counter
import logging

import pandas as pd
from pandas import DataFrame
from sklearn.metrics import cohen_kappa_score

from shared.utils import save_jsonl, load_json, save_json, mkf, mkd
from db.task import Task, DIR_ANNO, DIR_AREQ
from db import _task_dir

###############################################################################
# I chose to write it all on disk for now - we can change it to a db later.


def save_new_ar_for_user(task_id, user_id, annotation_requests, clean_existing=True):
    '''
    Save a list of new annotation requests for a user (annotator).
    Args:
        task_id: -
        user_id: -
        annotation_requests: A list of dict objects
        clean_existing: Optionally erase all existing requests
    '''
    # Save each file individually for fast random access.

    # TODO save to Redis?
    task = Task.fetch(task_id)

    basedir = [task.get_dir(), DIR_AREQ, str(user_id)]
    _basedir = os.path.join(*basedir)

    if clean_existing:
        # Clean out all old requests first
        if os.path.isdir(_basedir):
            shutil.rmtree(_basedir)

    mkd(*basedir)

    # NOTE insert these in reverse order so the most recently created ones are
    # the ones to be labeled first.
    annotation_requests = annotation_requests[::-1]

    # Save each annotation request request as a single file.
    for req in annotation_requests:
        path = basedir + [f"{req['ar_id']}.json"]
        fname = os.path.join(*path)
        save_json(fname, req)

        # TODO this is SUPER hacky. I want to maintain order of the annotations
        # because it would allow the most important datapoints to come first.
        # However, on some OS, file modified time granularity is not good enough.
        # So I space out all the saves a little bit.
        # This means saving annotations for each user (for 100 tasks) takes a full second!
        # This is a temporary fix.
        time.sleep(1/100.)

    # Return the dir holding all the AR's.
    return _basedir


def fetch_tasks_for_user(user_id):
    '''
    Return a list of task_id for which the user has annotation jobs to do.
    '''
    fnames = glob.glob(os.path.join(_task_dir(), '*', DIR_AREQ, user_id))
    task_ids = [re.match(f'^{_task_dir()}/(.*)/{DIR_AREQ}.*$', f).groups()[0]
                for f in fnames]
    return task_ids


def fetch_all_ar(task_id, user_id):
    '''
    Return a list of ar_id for this task
    '''
    _dir = os.path.join(_task_dir(task_id), DIR_AREQ, user_id)
    return _get_all_ar_ids_in_dir(_dir, sort_by_ctime=True)


def fetch_ar(task_id, user_id, ar_id):
    '''
    Return the details of a annotation request
    '''
    fname = os.path.join(_task_dir(task_id), DIR_AREQ,
                         user_id, ar_id + '.json')
    if os.path.isfile(fname):
        return load_json(fname)
    else:
        return None


def get_next_ar(task_id, user_id, ar_id):
    '''
    Get the next ar_id to show to user, or None.
    This function should be very fast.

    Logic:
        - If ar_id exist in list, get the next one that has not been labeled.
        - If ar_id does not exist, get the first one that has not been labeled.
        - If nothing left to label, return None
    '''
    ar_all = fetch_all_ar(task_id, user_id)
    ar_done = set(fetch_all_annotations(task_id, user_id))

    try:
        idx = ar_all.index(ar_id)
    except:
        idx = 0
    end = idx
    idx = (idx + 1) % len(ar_all)

    while idx != end:
        _ar_id = ar_all[idx]
        if _ar_id not in ar_done:
            return _ar_id
        idx = (idx + 1) % len(ar_all)

    return None


def build_empty_annotation(ar):
    return {
        'req': ar,
        'anno': {
            'labels': {}
        }
    }


def annotate_ar(task_id, user_id, ar_id, annotation):
    '''
    Annotate a annotation request
    '''
    ar = fetch_ar(task_id, user_id, ar_id)
    if ar is not None:
        path = [_task_dir(task_id), DIR_ANNO, user_id, ar_id + '.json']
        mkf(*path)
        fname = os.path.join(*path)
        anno = {
            'req': ar,
            'anno': annotation
        }
        save_json(fname, anno)
        return fname
    else:
        return None


def fetch_annotation(task_id, user_id, ar_id):
    '''
    Return the details of an annotation to a annotation request
    '''
    fname = os.path.join(_task_dir(task_id), DIR_ANNO,
                         user_id, ar_id + '.json')
    if os.path.isfile(fname):
        return load_json(fname)
    else:
        return None


def fetch_all_annotations(task_id, user_id):
    '''
    Return a list of ar_id for this task that has been annotated by this user.
    '''
    _dir = os.path.join(_task_dir(task_id), DIR_ANNO, user_id)
    return _get_all_ar_ids_in_dir(_dir)


def _get_all_ar_ids_in_dir(_dir, sort_by_ctime=False):
    if os.path.isdir(_dir):
        dir_entries = list(os.scandir(_dir))
        if sort_by_ctime:
            dir_entries = sorted(
                dir_entries, key=lambda x: x.stat().st_ctime_ns, reverse=True)
        fnames = [x.name for x in dir_entries]
        ar_ids = [re.match('(.*).json$', f).groups()[0]
                  for f in fnames]
        return ar_ids
    else:
        return []


def _get_all_annotators_from_requested(task_id):
    user_ids = []
    _path = os.path.join(_task_dir(task_id), DIR_AREQ)
    if os.path.isdir(_path):
        for dir_entry in os.scandir(_path):
            if os.path.isdir(dir_entry.path):
                user_ids.append(dir_entry.name)
    return user_ids


def _get_all_annotators_from_annotated(task_id):
    user_ids = []
    _path = os.path.join(_task_dir(task_id), DIR_ANNO)
    if os.path.isdir(_path):
        for dir_entry in os.scandir(_path):
            if os.path.isdir(dir_entry.path):
                user_ids.append(dir_entry.name)
    return user_ids


def compute_annotation_statistics(task_id):
    # How many have been labeled & How many are left to be labeled.

    n_annotations_per_label = defaultdict(lambda: defaultdict(int))
    n_annotations_per_user = defaultdict(lambda: 0)
    n_outstanding_requests_per_user = defaultdict(lambda: 0)

    user_ids = set(
        _get_all_annotators_from_requested(task_id) +
        _get_all_annotators_from_annotated(task_id)
    )

    total_distinct_annotations = set()
    annotations_from_all_users = []

    for user_id in user_ids:
        anno_ids = fetch_all_annotations(task_id, user_id)
        total_distinct_annotations.update(anno_ids)
        annotations_from_all_users.append(set(anno_ids))
        n_annotations_per_user[user_id] = len(anno_ids)

        # TODO: slow
        for anno_id in anno_ids:
            anno = fetch_annotation(task_id, user_id, anno_id)
            for label, result in anno['anno']['labels'].items():
                n_annotations_per_label[label][result] += 1

        # Only count the examples the user has not labeled yet.
        ar_ids = fetch_all_ar(task_id, user_id)
        n_outstanding_requests_per_user[user_id] = len(
            set(ar_ids) - set(anno_ids))

    # kappa stats calculation
    kappa_table_per_label = _calculate_per_label_kappa_stats_table(
        task_id, user_ids, annotations_from_all_users)

    return {
        'total_annotations': sum(n_annotations_per_user.values()),
        'total_distinct_annotations': len(total_distinct_annotations),
        'n_annotations_per_user': n_annotations_per_user,
        'n_annotations_per_label': n_annotations_per_label,
        'kappa_table_per_label': kappa_table_per_label,
        'total_outstanding_requests': sum(n_outstanding_requests_per_user.values()),
        'n_outstanding_requests_per_user': n_outstanding_requests_per_user,
    }


def _calculate_per_label_kappa_stats_table(task_id, user_ids,
                                           annotations_from_all_users):
    """Calculate per label kappa matrix stats.

    :param task_id: the id of a task
    :param user_ids: the user id list
    :param annotations_from_all_users: annotations from all users
    :return: the per label kappa matrix html table
    """
    if len(user_ids) == 1:
        return ['There is only one user {}'.format(list(user_ids)[0])]
    if len(annotations_from_all_users) == 0:
        return ['There are no annotations from any user yet.']
    annotation_intersection = set.intersection(*annotations_from_all_users)
    if len(annotation_intersection) == 0:
        return ['No overlapping annotations found among users.']
    kappa_stats_raw_data = _construct_per_label_per_user_result(
        task_id,
        user_ids,
        annotation_intersection
    )
    logging.info(kappa_stats_raw_data)
    kappa_matrices = _compute_kappa_matrix(user_ids, kappa_stats_raw_data)
    kappa_matrix_html_tables = _convert_html_tables(kappa_matrices)
    return kappa_matrix_html_tables


def _convert_html_tables(kappa_matrices):
    float_formatter = "{:.2f}".format
    kappa_html_tables = defaultdict(str)
    for label, df in kappa_matrices.items():
        kappa_html_tables[label] = df.to_html(classes='kappa_table', float_format=float_formatter)
    return kappa_html_tables


def _construct_per_label_per_user_result(task_id, user_ids,
                                         annotation_intersection):
    """Construct the per label per user labeling result dictionary.

    :param task_id: the id of a task
    :param user_ids: the user ids
    :param annotation_intersection: annotation ids of intersected annotations
    :return: a dictionary of per label per user labeling result

    Structure of the labeling results per label per user for the same set
    of annotations:

    {
        "label1": {
            "user_id1": [1, -1, 1, 1, -1],
            "user_id2": [-1, 1, 1, -1, 1],
            ...
        },
        "label12": {
            "user_id1": [1, -1, 1, -1, 1],
            "user_id2": [1, -1, -1, 1, 1],
            ...
        },
        ...
    }
    """

    kappa_stats_raw_data = defaultdict(lambda: defaultdict(lambda: []))
    for anno_id in annotation_intersection:
        for user_id in user_ids:
            anno = fetch_annotation(task_id, user_id, anno_id)
            for label, result in anno['anno']['labels'].items():
                kappa_stats_raw_data[label][user_id].append(result)

    return kappa_stats_raw_data


def _compute_kappa_matrix(user_ids, kappa_stats_raw_data):
    """Compute the kappa matrix for each label and return the html form of the
    matrix.

    :param user_ids: the user ids
    :param kappa_stats_raw_data: raw labeling results per label per user
    :return: a dictionary of kappa matrix html table per label

    Structure of the input:
    {
        "label1": {
            "user_id1": [1, -1, 1, 1, -1],
            "user_id2": [-1, 1, 1, -1, 1],
            ...
        },
        "label12": {
            "user_id1": [1, -1, 1, -1, 1],
            "user_id2": [1, -1, -1, 1, 1],
            ...
        },
        ...
    }

    Structure of the final output:
    {
        "label": html table form of the kappa matrix for this label
        ...
    }

    """
    all_pairs_of_users = list(itertools.combinations(user_ids, 2))
    kappa_matrix = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float)))
    for label, result_per_label in kappa_stats_raw_data.items():
        for user_pair in all_pairs_of_users:
            result_user1 = result_per_label[user_pair[0]]
            result_user2 = result_per_label[user_pair[1]]
            kappa_score = cohen_kappa_score(result_user1, result_user2)
            kappa_matrix[label][user_pair[0]][user_pair[1]] = kappa_score
            kappa_matrix[label][user_pair[1]][user_pair[0]] = kappa_score
            kappa_matrix[label][user_pair[0]][user_pair[0]] = 1
            kappa_matrix[label][user_pair[1]][user_pair[1]] = 1

    kappa_dataframe = defaultdict(DataFrame)
    for label, nested_dict in kappa_matrix.items():
        kappa_dataframe[label] = pd.DataFrame.from_dict(nested_dict)

    return kappa_dataframe


def _majority_label(labels):
    '''
    Get the majority of non-zero labels
    Input: [1,1,0,0,0,0,-1,-1,1,1]
    Output: 1
    '''
    labels = [x for x in labels if x != 0]
    if len(labels) > 0:
        return Counter(labels).most_common()[0][0]
    else:
        return None


def _export_labeled_examples(annotations_iterator):
    """
    Inputs:
        annotations_iterator: A iterator that returns annotations.

    An example annotation looks like:
        {
            'req': {
                'ar_id': '...'
                'data': {
                    'text': '...'
                }
            },
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                    'POP_HEALTH': -1,
                    'AI': 0,
                }
            }
        }

    Step 1. Labels will start off being gathered for each ar_id:
    {
        'ar_id_12345': {
            'HEALTHCARE': [1, 1, -1, 1, 0, 0]
        },
        ...
    }

    Step 2. Then they're merged by a merging strategy (currently majority vote):
    {
        'ar_id_12345': {
            'HEALTHCARE': 1
        },
        ...
    }

    Step 3. Finally we join labels with the text using ar_id.
    [
        {
            'text': '...',
            'labels': {'HEALTHCARE': 1}
        },
        ...
    ]

    This function returns result from the last step.
    """

    text = {}
    labels = defaultdict(lambda: defaultdict(list))

    # Step 1. Gather all the labels (and text)
    for anno in annotations_iterator:
        ar_id = anno['req']['ar_id']

        text[ar_id] = anno['req']['data'].get('text') or ''

        for label_key, label_value in anno['anno']['labels'].items():
            # e.g. label_key = 'HEALTHCARE', label_value = 1
            labels[ar_id][label_key].append(label_value)

    # Step 2. Merge all labels within each ar_id
    new_labels = {}
    for ar_id in labels:
        new_labels[ar_id] = {
            label_key: _majority_label(list_of_label_values)
            for label_key, list_of_label_values in labels[ar_id].items()
            if _majority_label(list_of_label_values) is not None
        }
    labels = new_labels

    # Step 3. Join with text on ar_id
    final = []
    for ar_id in labels:
        # If any labels are left - An example could have the "unsure" label for
        # all its annotations, and since we remove all unsure labels, there
        # might be any labels left.
        if len(labels[ar_id]) > 0:
            final.append({
                'text': text[ar_id],
                'labels': labels[ar_id]
            })

    return final


def export_labeled_examples(task_id, outfile=None):
    def annotations_iterator():
        for user_id in _get_all_annotators_from_annotated(task_id):
            for ar_id in fetch_all_annotations(task_id, user_id):
                anno = fetch_annotation(task_id, user_id, ar_id)

                yield anno

    final = _export_labeled_examples(annotations_iterator())

    if outfile is not None:
        save_jsonl(outfile, final)

    return final
