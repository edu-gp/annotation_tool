import math
from collections import namedtuple

import pandas as pd
import numpy as np
from sqlalchemy import distinct, func

from tests.sqlalchemy_conftest import *
from ar.data import _compute_kappa_matrix, \
    _compute_number_of_annotations_done_per_user, \
    _exclude_unknowns_for_kappa_calculation, \
    _retrieve_annotation_with_same_entity_shared_by_two_users, \
    _construct_kappa_stats_raw_data, \
    _retrieve_entity_ids_and_annotation_values_by_user, \
    EntityAndAnnotationValuePair, compute_annotation_request_statistics, \
    _compute_total_distinct_number_of_annotated_entities_for_label, \
    _compute_num_of_annotations_per_value, PrettyDefaultDict, \
    fetch_annotated_ar_ids_from_db, fetch_ar_ids, construct_ar_request_dict, \
    _construct_comparison_df
from db.model import User, ClassificationAnnotation, \
    AnnotationRequest, AnnotationType, AnnotationRequestStatus, Task, \
    update_instance, AnnotationValue

ENTITY_TYPE = 'blah'


def test__exclude_unknowns_for_kappa_calculation():
    input1 = [1, 1, 1, -1]
    input2 = [-1, 1, 1, -1]

    result1, result2 = _exclude_unknowns_for_kappa_calculation(input1, input2)
    assert input1 == result1
    assert input2 == result2

    input1 = [1, 0, 1, 0]
    input2 = [0, -1, -1, 0]
    result1, result2 = _exclude_unknowns_for_kappa_calculation(input1, input2)
    assert result1 == [1]
    assert result2 == [-1]


def test__compute_kappa_matrix():
    raw_data = {
        "label1": {
            ("user_id1", "user_id2"): {
                "user_id1": [1, -1, 1, 1, -1],
                "user_id2": [-1, 1, 1, -1, 1],
            }
        },
        "label2": {
            ("user_id1", "user_id2"): {
                "user_id1": [1, -1, 1, -1, 1],
                "user_id2": [1, -1, -1, 1, 1],
            }
        },
    }
    user_ids = ["user_id1", "user_id2"]
    kappa_matrix = _compute_kappa_matrix(
        kappa_stats_raw_data=raw_data
    )
    assert (len(kappa_matrix) == len(raw_data))
    for label in kappa_matrix:
        matrix_per_label = kappa_matrix[label]
        assert (len(matrix_per_label) == len(user_ids))
        for user in matrix_per_label:
            assert (len(matrix_per_label[user]) == len(user_ids))
            assert (matrix_per_label[user][user] == 1)


def test__compute_kappa_matrix_unknown():
    raw_data = {
        "label1": {
            ("user_id1", "user_id2"): {
                "user_id1": [1, 0, 1, 1, -1],
                "user_id2": [0, 0, 0, 0, 0],
            }
        },
    }
    user_ids = ["user_id1", "user_id2"]
    kappa_matrix = _compute_kappa_matrix(
        kappa_stats_raw_data=raw_data
    )
    assert (len(kappa_matrix) == len(raw_data))
    for label in kappa_matrix:
        matrix_per_label = kappa_matrix[label]
        assert (len(matrix_per_label) == len(user_ids))
        for user in matrix_per_label:
            assert (len(matrix_per_label[user]) == len(user_ids))
            for user_id in user_ids:
                if user == user_id:
                    assert (matrix_per_label[user][user_id] == 1)
                else:
                    assert math.isnan(matrix_per_label[user][user_id])


def _populate_annotation_data(dbsession):
    username1 = "ooo"
    username2 = "ppp"
    username3 = "qqq"
    label1 = "whatever"
    label2 = "Not Whatever"

    user1 = User(username=username1)
    user2 = User(username=username2)
    user3 = User(username=username3)
    # label = Label(name=label_name)

    entity1 = "1. whatever"
    entity2 = "2. SQL is fun."
    entity3 = "3. Blahblah."

    dbsession.add_all([user1, user2, user3])
    dbsession.commit()

    # A1 and A2 from user1 has the same entity with A4 and A5 from user2.
    # A3 from user1 has the same entity with A6 from user3.
    annotation1 = ClassificationAnnotation(value=1, user_id=user1.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity1)
    annotation2 = ClassificationAnnotation(value=1, user_id=user1.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity2)
    annotation3 = ClassificationAnnotation(value=-1, user_id=user1.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity3)

    annotation4 = ClassificationAnnotation(value=1, user_id=user2.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity1)
    annotation5 = ClassificationAnnotation(value=-1, user_id=user2.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity2)
    annotation6 = ClassificationAnnotation(value=-1, user_id=user3.id,
                                           label=label1,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity3)
    annotation7 = ClassificationAnnotation(value=-1, user_id=user3.id,
                                           label=label2,
                                           entity_type=ENTITY_TYPE,
                                           entity=entity3)

    annotations = [
        annotation1, annotation2, annotation3, annotation4, annotation5,
        annotation6, annotation7
    ]
    dbsession.add_all(annotations)
    dbsession.commit()

    return user1, user2, user3, entity1, entity2, entity3, label1, label2,\
        annotations


def test__compute_num_of_annotations_per_value(dbsession):
    user1, user2, user3, _, _, _, label1, label2, annotations = \
        _populate_annotation_data(dbsession)

    for label in [label1, label2]:
        num_of_annotations_per_value = _compute_num_of_annotations_per_value(
            dbsession=dbsession, label=label
        )

        expected = PrettyDefaultDict(lambda: 0)
        for annotation in annotations:
            if annotation.label == label:
                expected[annotation.value] += 1
        assert num_of_annotations_per_value == expected


def test__compute_total_annotations(dbsession):
    user1, user2, user3, _, _, _, label1, label2, annotations = \
        _populate_annotation_data(dbsession)

    for label in [label1, label2]:
        total_distinct_annotated_entities = \
            _compute_total_distinct_number_of_annotated_entities_for_label(
                dbsession=dbsession, label=label
            )

        if label == label1:
            assert total_distinct_annotated_entities == 3
        elif label == label2:
            assert total_distinct_annotated_entities == 1

        expected = PrettyDefaultDict(lambda: 0)
        UserNameIdPair = namedtuple('UserNameIdPair', ['name', 'id'])
        for annotation in annotations:
            if annotation.label == label:
                expected[UserNameIdPair(annotation.user.username,
                                        annotation.user_id)] += 1
        res = _compute_number_of_annotations_done_per_user(
            dbsession=dbsession, label=label
        )
        for num, name, user_id in res:
            assert expected[UserNameIdPair(name, user_id)] == num


def test__construct_kappa_stats_raw_data(dbsession):
    user1, user2, user3, entity1, entity2, entity3, label1, label2, _ \
        = _populate_annotation_data(dbsession)

    res = _retrieve_entity_ids_and_annotation_values_by_user(
        dbsession=dbsession, users=[user1, user2, user3], label=label1)
    assert res == {
        user1.id: [
            EntityAndAnnotationValuePair(entity1, 1),
            EntityAndAnnotationValuePair(entity2, 1),
            EntityAndAnnotationValuePair(entity3, -1)],
        user2.id: [
            EntityAndAnnotationValuePair(entity1, 1),
            EntityAndAnnotationValuePair(entity2, -1)],
        user3.id: [EntityAndAnnotationValuePair(entity3, -1)]}

    res1 = _retrieve_annotation_with_same_entity_shared_by_two_users(
        user1=user1, user2=user2, entities_and_annotation_values_by_user=res
    )
    assert res1 == {
        user1.username: [1, 1],
        user2.username: [1, -1]
    }

    res2 = _retrieve_annotation_with_same_entity_shared_by_two_users(
        user1=user1, user2=user3, entities_and_annotation_values_by_user=res
    )
    assert res2 == {
        user1.username: [-1],
        user3.username: [-1]
    }

    res3 = _retrieve_annotation_with_same_entity_shared_by_two_users(
        user1=user2, user2=user3, entities_and_annotation_values_by_user=res
    )
    assert res3 is None

    kappa_raw_data = _construct_kappa_stats_raw_data(
        dbsession=dbsession, distinct_users={user1, user2, user3},
        label=label1)

    assert kappa_raw_data == {
        label1: {
            tuple(sorted([user1.username, user2.username])): res1,
            tuple(sorted([user1.username, user3.username])): res2,
            tuple(sorted([user2.username, user3.username])): res3
        }
    }


def _populate_annotation_requests(dbsession):
    username1 = "ooo"
    username2 = "ppp"
    taskname1 = "task1"
    taskname2 = "task2"
    default_params = {"whatever": 1}
    request_name1 = "name1"
    request_name2 = "name2"
    request_name3 = "name3"
    context1 = {
        'fname': 'fname1',
        'line_number': 1,
        'score': 0.98,
        'source': 'testing',
        "data": {
            "text": "Blah blah ...",
            "meta": {"name": "Blah", "domain": "blah"}
        },
        "pattern_info": {
            "tokens": ["Blah", "blah"],
            "matches": [(1, 2, "Blah")],
            "score": 0.11627906976744186
        }
    }

    user1 = User(username=username1)
    user2 = User(username=username2)

    entity1 = "whatever"
    entity2 = "SQL is fun."
    entity3 = "Blahblah."

    task1 = Task(name=taskname1, default_params=default_params)
    task2 = Task(name=taskname2, default_params=default_params)

    dbsession.add_all([user1, user2, task1, task2])
    dbsession.commit()

    # Requests for user 1
    request1 = AnnotationRequest(
        user_id=user1.id,
        entity_type=ENTITY_TYPE,
        entity=entity1,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id,
        name=request_name1,
        context=context1
    )

    request2 = AnnotationRequest(
        user_id=user1.id,
        entity_type=ENTITY_TYPE,
        entity=entity2,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Complete,
        task_id=task1.id,
        name=request_name2
    )

    request3 = AnnotationRequest(
        user_id=user1.id,
        entity_type=ENTITY_TYPE,
        entity=entity3,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Stale,
        task_id=task1.id,
        name=request_name3
    )

    # Requests for user 2
    request4 = AnnotationRequest(
        user_id=user2.id,
        entity_type=ENTITY_TYPE,
        entity=entity1,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id
    )

    request5 = AnnotationRequest(
        user_id=user2.id,
        entity_type=ENTITY_TYPE,
        entity=entity2,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id
    )

    request6 = AnnotationRequest(
        user_id=user2.id,
        entity_type=ENTITY_TYPE,
        entity=entity3,
        label='hi',
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Complete,
        task_id=task1.id
    )

    requests = [request1, request2, request3, request4, request5, request6]
    dbsession.add_all(requests)
    dbsession.commit()
    return task1, task2, user1, user2, requests


def test_compute_annotation_request_statistics(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)

    res = compute_annotation_request_statistics(dbsession, task1.id)

    expected = PrettyDefaultDict(lambda: 0)
    for request in requests:
        if request.status == AnnotationRequestStatus.Pending:
            expected[request.user.username] += 1
    assert res['total_outstanding_requests'] == sum(expected.values())
    assert res['n_outstanding_requests_per_user'] == {
        user1.username: expected[user1.username],
        user2.username: expected[user2.username]
    }


def test_fetch_ar_ids(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    res = fetch_ar_ids(dbsession=dbsession, task_id=task1.id,
                       username=user1.username)

    for request in requests:
        if request.task_id == task1.id and request.user.username == \
                user1.username:
            assert request.id in set(res)
        else:
            assert request.id not in set(res)


def test_fetch_annotated_ar_ids_from_db(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    res = fetch_annotated_ar_ids_from_db(dbsession=dbsession,
                                         task_id=task1.id,
                                         username=user1.username)

    for request in requests:
        if request.task_id == task1.id and request.user.username == \
                user1.username and request.status == \
                AnnotationRequestStatus.Complete:
            assert request.id in set(res)
        else:
            assert request.id not in set(res)


def test_update_instance(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    assert task1.name == "task1"
    update_instance(dbsession=dbsession,
                    model=Task,
                    filter_by_dict={"id": task1.id},
                    update_dict={"name": "task_updated"})
    assert task1.name == "task_updated"


def test_construct_ar_request_dict(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    request1 = requests[0]
    result1 = construct_ar_request_dict(dbsession, request1.id)
    assert result1 == {
        'ar_id': request1.id,
        'fname': request1.context['fname'],
        'line_number': request1.context['line_number'],
        'score': request1.context['score'],
        'data': request1.context['data'],
        'pattern_info': request1.context['pattern_info'],
        'entity': request1.entity,
        'entity_type': ENTITY_TYPE,
        'label': request1.label
    }

    request2 = requests[1]
    result2 = construct_ar_request_dict(dbsession, request2.id)
    assert result2 == {
        'ar_id': request2.id,
        'fname': None,
        'line_number': None,
        'score': None,
        'data': None,
        'pattern_info': None,
        'entity': request2.entity,
        'entity_type': ENTITY_TYPE,
        'label': request2.label
    }


def test__construct_comparison_df(dbsession):
    user1 = User(username="user1")
    user2 = User(username="user2")
    user3 = User(username="user3")

    dbsession.add_all([user1, user2, user3])
    dbsession.commit()

    label = "label1"
    entity1 = "entity1"
    entity2 = "entity2"
    entity_type = "company"

    # Annotations from user1
    annotation1 = ClassificationAnnotation(
        entity=entity1,
        entity_type=entity_type,
        label=label,
        value=AnnotationValue.POSITIVE,
        user_id=user1.id
    )

    annotation2 = ClassificationAnnotation(
        entity=entity2,
        entity_type=entity_type,
        label=label,
        value=AnnotationValue.POSITIVE,
        user_id=user1.id
    )

    # Annotations from user2
    annotation3 = ClassificationAnnotation(
        entity=entity1,
        entity_type=entity_type,
        label=label,
        value=AnnotationValue.POSITIVE,
        user_id=user2.id
    )

    annotation4 = ClassificationAnnotation(
        entity=entity2,
        entity_type=entity_type,
        label=label,
        value=AnnotationValue.NEGTIVE,
        user_id=user2.id
    )

    # Annotations from user3
    annotation5 = ClassificationAnnotation(
        entity=entity1,
        entity_type=entity_type,
        label=label,
        value=AnnotationValue.UNSURE,
        user_id=user3.id
    )

    dbsession.add_all([annotation1, annotation2, annotation3, annotation4,
                       annotation5])
    dbsession.commit()

    comparison_df, id_df = _construct_comparison_df(
        dbsession=dbsession,
        label=label,
        users_to_compare=[user1.username,
                          user2.username,
                          user3.username])

    expected_comparison_df = pd.DataFrame({
        user1.username: [str(annotation2.value), str(annotation1.value)],
        user2.username: [str(annotation4.value), str(annotation3.value)],
        user3.username: [str(np.NaN), str(annotation5.value)]
    }, index=[entity2, entity1])

    expected_id_df = pd.DataFrame({
        user1.username: [str(annotation2.id), str(annotation1.id)],
        user2.username: [str(annotation4.id), str(annotation3.id)],
        user3.username: [str(np.NaN), str(annotation5.id)]
    }, index=[entity2, entity1])

    assert(comparison_df.equals(expected_comparison_df))
    assert(id_df.equals(expected_id_df))






