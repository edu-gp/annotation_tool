import glob
import itertools
import logging
import os
import re
import shutil
import time
from collections import defaultdict, Counter, namedtuple
from typing import Tuple, Dict

import pandas as pd
from pandas import DataFrame
from sklearn.metrics import cohen_kappa_score
from sqlalchemy import func

from db import _task_dir
from db.model import db, Label, User, ClassificationAnnotation, \
    AnnotationRequest, AnnotationRequestStatus, Context
from db.task import Task, DIR_ANNO, DIR_AREQ
from shared.utils import save_jsonl, load_json, save_json, mkf, mkd, \
    PrettyDefaultDict

###############################################################################
# I chose to write it all on disk for now - we can change it to a db later.

# Utility namedtuples
UserNameAndIdPair = namedtuple('UserNameAndIdPair', ['username', 'id'])
EntityAndAnnotationValuePair = namedtuple(
    'EntityAndAnnotationValuePair', ['entity_id', 'value'])


def save_new_ar_for_user(task_id, user_id, annotation_requests,
                         clean_existing=True):
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
        time.sleep(1 / 100.)

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


def fetch_ar_names(dbsession, task_id, username):
    res = dbsession.query(AnnotationRequest.name).join(User).filter(
        User.username == username, AnnotationRequest.task_id == task_id).all()
    return [
        item[0] for item in res
    ]


# TODO delete after the migration to db is done.
def fetch_all_ar(task_id, username):
    '''
    Return a list of ar_id for this task
    '''
    _dir = os.path.join(_task_dir(task_id), DIR_AREQ, username)
    return _get_all_ar_ids_in_dir(_dir, sort_by_ctime=True)


def fetch_ar_by_name_from_db(dbsession, task_id, user_id, ar_name) -> Dict:
    request_id, request_additional_info, context_id, context_data = \
        dbsession.query(AnnotationRequest.id,
                        AnnotationRequest.additional_info,
                        Context.id,
                        Context.data).\
        join(Context).\
        filter(
            AnnotationRequest.task_id == task_id,
            AnnotationRequest.name == ar_name,
            AnnotationRequest.user_id == user_id
        ).one_or_none()
    return _construct_ar_request_json(request_id, request_additional_info,
                                      context_id, context_data)


def _construct_ar_request_json(ar_id, ar_additional_info, context_id,
                               context_data):
    return {
        'ar_id': ar_id,
        'fname': ar_additional_info.get('fname', None),
        'line_number': ar_additional_info.get('line_number', None),
        'score': ar_additional_info.get('score', None),
        'data': context_data,
        'context_id': context_id
    }



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


def get_next_ar_name_from_db(dbsession, task_id, user_id, current_ar_id):
    return dbsession.query(AnnotationRequest.name).filter(
        AnnotationRequest.task_id == task_id,
        AnnotationRequest.user_id == user_id,
        AnnotationRequest.id > current_ar_id
    ).order_by(AnnotationRequest.id.asc()).first()


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
    ar_done = set(fetch_all_ar_ids(task_id, user_id))

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


def fetch_user_id_by_username(dbsession, username):
    return dbsession.query(User.id).filter(User.username ==
                                           username).one_or_none()

def fetch_existing_annotation_from_db(dbsession, user_id, context_id, label):
    pass


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


def fetch_annotated_ar_names_from_db(dbsession, task_id, username):
    res = dbsession.query(AnnotationRequest.name).join(User).filter(
        AnnotationRequest.task_id == task_id,
        AnnotationRequest.status == AnnotationRequestStatus.Complete,
        User.username == username
    ).all()

    return [item[0] for item in res]


def fetch_all_ar_ids(task_id, user_id):
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

    total_distinct_annotations = _gather_distinct_labeled_examples(task_id)
    anno_ids_per_user = dict()

    results_per_task_user_anno_id = dict()

    for user_id in user_ids:
        anno_ids = fetch_all_ar_ids(task_id, user_id)
        anno_ids_per_user[user_id] = set(anno_ids)
        n_annotations_per_user[user_id] = len(anno_ids)

        # TODO: slow
        for anno_id in anno_ids:
            anno = fetch_annotation(task_id, user_id, anno_id)
            results_per_task_user_anno_id[(task_id, user_id, anno_id)] = anno
            for label, result in anno['anno']['labels'].items():
                n_annotations_per_label[label][result] += 1

        # Only count the examples the user has not labeled yet.
        ar_ids = fetch_all_ar(task_id, user_id)
        n_outstanding_requests_per_user[user_id] = len(
            set(ar_ids) - set(anno_ids))

    kappa_table_per_label = {}

    return {
        'total_annotations': sum(n_annotations_per_user.values()),
        'total_distinct_annotations': len(total_distinct_annotations),
        'n_annotations_per_user': n_annotations_per_user,
        'n_annotations_per_label': n_annotations_per_label,
        'kappa_table_per_label': kappa_table_per_label,
        'total_outstanding_requests': sum(
            n_outstanding_requests_per_user.values()),
        'n_outstanding_requests_per_user': n_outstanding_requests_per_user,
    }


def compute_annotation_request_statistics(dbsession, task_id):
    total_number_of_outstanding_requests = dbsession.query(AnnotationRequest).\
        filter(
            AnnotationRequest.task_id == task_id,
            AnnotationRequest.status == AnnotationRequestStatus.Pending
    ).count()

    n_outstanding_requests_per_user = dbsession.query(
        func.count(AnnotationRequest.id),
        User.username
    ). \
        join(User). \
        filter(AnnotationRequest.status == AnnotationRequestStatus.Pending). \
        group_by(User.username).all()
    n_outstanding_requests_per_user_dict = {
        username: num
        for num, username in n_outstanding_requests_per_user
    }

    return {
        'total_outstanding_requests': total_number_of_outstanding_requests,
        'n_outstanding_requests_per_user': n_outstanding_requests_per_user_dict
    }


def compute_annotation_statistics_db(dbsession, label_name):
    total_distinct_annotations = \
        _compute_total_distinct_number_of_annotations_for_label(
            dbsession=dbsession,
            label_name=label_name
        )

    num_of_annotations_done_per_user = \
        _compute_number_of_annotations_done_per_user(
            dbsession=dbsession,
            label_name=label_name
        )

    total_num_of_annotations_done_by_users = sum(
        [num for num, username, user_id in num_of_annotations_done_per_user])
    n_annotations_done_per_user_dict = {
        username: num
        for num, username, user_id in num_of_annotations_done_per_user
    }

    num_of_annotations_per_value = _compute_num_of_annotations_per_value(
        dbsession=dbsession, label_name=label_name
    )

    # kappa stats calculation
    distinct_users = set([
        UserNameAndIdPair(username=item[1], id=item[2]) for item in
        num_of_annotations_done_per_user
    ])

    kappa_stats_raw_data = _construct_kappa_stats_raw_data(
        db.session, distinct_users, label_name)

    kappa_table_per_label = _convert_html_tables(
        kappa_matrices=_compute_kappa_matrix(kappa_stats_raw_data)
    )

    return {
        'total_annotations': total_num_of_annotations_done_by_users,
        'total_distinct_annotations': total_distinct_annotations,
        'n_annotations_per_value': num_of_annotations_per_value,
        'n_annotations_per_user': n_annotations_done_per_user_dict,
        'kappa_table': kappa_table_per_label,
    }


def _compute_num_of_annotations_per_value(dbsession, label_name):
    res = dbsession.query(
        func.count(ClassificationAnnotation.id),
        ClassificationAnnotation.value
    ).\
        join(Label).\
        filter(Label.name == label_name).\
        group_by(ClassificationAnnotation.value).all()
    data = PrettyDefaultDict(lambda: 0)
    for item in res:
        data[item[1]] = item[0]
    return data


def _compute_total_distinct_number_of_annotations_for_label(dbsession,
                                                            label_name):
    total_distinct_annotations = dbsession.query(
        ClassificationAnnotation).join(Label).filter(
        Label.name == label_name
    ).count()
    return total_distinct_annotations


def _compute_number_of_annotations_done_per_user(dbsession, label_name):
    num_of_annotations_done_per_user = dbsession.query(
        func.count(ClassificationAnnotation.id),
        User.username,
        User.id
    ). \
        join(User). \
        join(Label). \
        filter(Label.name == label_name). \
        group_by(User.username).all()

    return num_of_annotations_done_per_user


def _construct_kappa_stats_raw_data(dbsession, distinct_users, label_name):
    entities_and_annotation_values_by_user = \
        _retrieve_entity_ids_and_annotation_values_by_user(dbsession,
                                                           distinct_users)
    user_pairs = list(itertools.combinations(distinct_users, 2))
    kappa_stats_raw_data = {
        label_name: {
            tuple(sorted([user_pair[0].username, user_pair[1].username])):
                _retrieve_annotation_with_same_entity_shared_by_two_users(
                    user_pair[0], user_pair[1],
                    entities_and_annotation_values_by_user)
            for user_pair in user_pairs
        }
    }
    return kappa_stats_raw_data


def _retrieve_entity_ids_and_annotation_values_by_user(dbsession, users):
    res = dbsession.query(
        ClassificationAnnotation.entity_id,
        ClassificationAnnotation.value,
        ClassificationAnnotation.user_id
    ). \
        filter(ClassificationAnnotation.user_id.in_(
            [user.id for user in users]
        )
    ).all()

    data = PrettyDefaultDict(lambda: [])
    for item in res:
        data[item[2]].append(EntityAndAnnotationValuePair(
            entity_id=item[0],
            value=item[1]
        ))
    return data


def _retrieve_annotation_with_same_entity_shared_by_two_users(
        user1, user2, entities_and_annotation_values_by_user):
    annotations_from_user1 = entities_and_annotation_values_by_user[user1.id]
    annotations_from_user2 = entities_and_annotation_values_by_user[user2.id]

    dict_of_context_value_from_user1 = {
        annotation.entity_id: annotation.value
        for annotation in annotations_from_user1
    }

    dict_of_context_value_from_user2 = {
        annotation.entity_id: annotation.value
        for annotation in annotations_from_user2
    }
    intersection = set(dict_of_context_value_from_user1.keys()).intersection(
        set(dict_of_context_value_from_user2.keys()))
    intersection = list(intersection)

    if len(intersection) == 0:
        return None

    values_from_annotations_with_overlapping_context_user1 = [
        dict_of_context_value_from_user1[entity_id] for entity_id in
        intersection
    ]

    values_from_annotations_with_overlapping_context_user2 = [
        dict_of_context_value_from_user2[entity_id] for entity_id in
        intersection
    ]

    return {
        user1.username: values_from_annotations_with_overlapping_context_user1,
        user2.username: values_from_annotations_with_overlapping_context_user2
    }


def _compute_kappa_matrix(kappa_stats_raw_data):
    """Compute the kappa matrix for each label and return the html form of the
    matrix.

    :param user_ids: the user ids
    :param kappa_stats_raw_data: raw labeling results per label per user pair
    on overlapping annotations
    :return: a dictionary of kappa matrix html table per label

    Structure of the input:
    {
        "label1": {
            ("user_id1", "user_id2"): {
                "user_id1": [1, -1, 1, 1, -1],
                "user_id2": [-1, 1, 1, -1, 1]
            },
            ("user_id1", "user_id3"): {
                "user_id1": [1, -1],
                "user_id2": [-1, 1]
            }
            ...
        },
        "label12": {
            ("user_id1", "user_id3"): {
                "user_id1": [1, -1],
                "user_id2": [-1, 1]
            },
            ...
        },
        ...
    }

    Structure of the final output:
    {
        "label1": kappa matrix for this label as a pandas dataframe,
        "label2": kappa matrix for this label as a pandas dataframe,
        ...
    }

    """
    kappa_matrix = PrettyDefaultDict(
        lambda: PrettyDefaultDict(lambda: PrettyDefaultDict(float)))
    for label, result_per_user_pair_per_label in kappa_stats_raw_data.items():
        for user_pair, result_per_user in \
                result_per_user_pair_per_label.items():
            if result_per_user is None:
                continue
            result_user1 = result_per_user[user_pair[0]]
            result_user2 = result_per_user[user_pair[1]]
            logging.error("Calculating the kappa score for {} and {}".format(
                user_pair[0], user_pair[1]))
            result_user1, result_user2 = \
                _exclude_unknowns_for_kappa_calculation(result_user1,
                                                        result_user2)
            kappa_score = cohen_kappa_score(result_user1, result_user2)
            kappa_matrix[label][user_pair[0]][user_pair[1]] = kappa_score
            kappa_matrix[label][user_pair[1]][user_pair[0]] = kappa_score
            kappa_matrix[label][user_pair[0]][user_pair[0]] = 1
            kappa_matrix[label][user_pair[1]][user_pair[1]] = 1

    kappa_dataframe = PrettyDefaultDict(DataFrame)
    for label, nested_dict in kappa_matrix.items():
        kappa_dataframe[label] = pd.DataFrame.from_dict(nested_dict)
    return kappa_dataframe


def _exclude_unknowns_for_kappa_calculation(result_user1, result_user2):
    """Exclude unknowns for kappa calculation.

    This means if either of the user's label result is unknown then we
    should include neither in the final calculation.

    :param result_user1: labeling results from user1 with potentially unknowns
    :param result_user2: labeling results from user2 with potentially unknowns
    :return: the labeling result tuple without unknowns
    """
    if len(result_user1) != len(result_user2):
        raise ValueError("The number of labeling results should be the same.")
    labeling_results1 = []
    labeling_results2 = []
    ignored_count = 0
    for i in range(len(result_user1)):
        if result_user1[i] != 0 and result_user2[i] != 0:
            labeling_results1.append(result_user1[i])
            labeling_results2.append(result_user2[i])
        else:
            ignored_count += 1
    logging.error("Unknown ignored count: {}".format(ignored_count))
    return labeling_results1, labeling_results2


def _convert_html_tables(kappa_matrices):
    float_formatter = "{:.2f}".format
    kappa_html_tables = PrettyDefaultDict(str)
    for label, df in kappa_matrices.items():
        kappa_html_tables[label] = df.to_html(classes='kappa_table',
                                              float_format=float_formatter)
    return kappa_html_tables


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


def _export_distinct_labeled_examples(annotations_iterator):
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


def _gather_distinct_labeled_examples(task_id):
    def annotations_iterator():
        for user_id in _get_all_annotators_from_annotated(task_id):
            for ar_id in fetch_all_ar_ids(task_id, user_id):
                anno = fetch_annotation(task_id, user_id, ar_id)

                yield anno

    final = _export_distinct_labeled_examples(annotations_iterator())
    return final


def export_labeled_examples(task_id, outfile=None):
    final = _gather_distinct_labeled_examples(task_id)

    if outfile is not None:
        save_jsonl(outfile, final)

    return final
