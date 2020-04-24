import math
from collections import namedtuple

from sqlalchemy import distinct

from tests.sqlalchemy_conftest import *
from ar.data import _compute_kappa_matrix, \
    _compute_number_of_annotations_done_per_user, \
    _exclude_unknowns_for_kappa_calculation, \
    _retrieve_annotation_with_same_entity_shared_by_two_users, \
    _construct_kappa_stats_raw_data, \
    _retrieve_entity_ids_and_annotation_values_by_user, \
    EntityAndAnnotationValuePair, compute_annotation_request_statistics, \
    _compute_total_distinct_number_of_annotations_for_label, \
    _compute_num_of_annotations_per_value, PrettyDefaultDict, fetch_ar_by_name_from_db, \
    fetch_annotated_ar_names_from_db, fetch_ar_names
from db.model import User, ClassificationAnnotation, Label, Entity, \
    AnnotationRequest, AnnotationType, AnnotationRequestStatus, Task


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
    label_name = "whatever"
    text1 = "whatever"
    text2 = "SQL is no fun."
    text3 = "Blahblah."

    user1 = User(username=username1)
    user2 = User(username=username2)
    user3 = User(username=username3)
    label = Label(name=label_name)

    entity1 = Entity(name=text1, entity_type_id=1)
    entity2 = Entity(name=text2, entity_type_id=1)
    entity3 = Entity(name=text3, entity_type_id=1)

    dbsession.add_all([user1, user2, user3, label,
                       entity1, entity2, entity3])
    dbsession.commit()

    # A1 and A2 from user1 has the same entity with A4 and A5 from user2.
    # A3 from user1 has the same entity with A6 from user3.
    annotation1 = ClassificationAnnotation(value=1, user_id=user1.id,
                                           label_id=label.id,
                                           entity_id=entity1.id)
    annotation2 = ClassificationAnnotation(value=1, user_id=user1.id,
                                           label_id=label.id,
                                           entity_id=entity2.id)
    annotation3 = ClassificationAnnotation(value=-1, user_id=user1.id,
                                           label_id=label.id,
                                           entity_id=entity3.id)

    annotation4 = ClassificationAnnotation(value=1, user_id=user2.id,
                                           label_id=label.id,
                                           entity_id=entity1.id)
    annotation5 = ClassificationAnnotation(value=-1, user_id=user2.id,
                                           label_id=label.id,
                                           entity_id=entity2.id)
    annotation6 = ClassificationAnnotation(value=-1, user_id=user3.id,
                                           label_id=label.id,
                                           entity_id=entity3.id)
    annotations = [
        annotation1, annotation2, annotation3, annotation4, annotation5,
        annotation6
    ]
    dbsession.add_all([annotation1, annotation2, annotation3,
                       annotation4, annotation5, annotation6])
    dbsession.commit()

    return user1, user2, user3, entity1, entity2, entity3, label, \
        annotations


def test__compute_num_of_annotations_per_value(dbsession):
    user1, user2, user3, _, _, _, label, annotations = \
        _populate_annotation_data(dbsession)

    num_of_annotations_per_value = _compute_num_of_annotations_per_value(
        dbsession=dbsession, label_name=label.name
    )

    expected = PrettyDefaultDict(lambda: 0)
    for annotation in annotations:
        expected[annotation.value] += 1
    assert num_of_annotations_per_value == expected


def test__compute_total_annotations(dbsession):
    user1, user2, user3, _, _, _, label, annotations = \
        _populate_annotation_data(dbsession)

    total_distinct_annotations = \
        _compute_total_distinct_number_of_annotations_for_label(
            dbsession=dbsession, label_name=label.name
        )

    assert total_distinct_annotations == len(annotations)

    expected = PrettyDefaultDict(lambda: 0)
    UserNameIdPair = namedtuple('UserNameIdPair', ['name', 'id'])
    for annotation in annotations:
        expected[UserNameIdPair(annotation.user.username,
                                annotation.user_id)] += 1
    res = _compute_number_of_annotations_done_per_user(
        dbsession=dbsession, label_name=label.name
    )
    for num, name, user_id in res:
        assert expected[UserNameIdPair(name, user_id)] == num


def test__construct_kappa_stats_raw_data(dbsession):
    user1, user2, user3, entity1, entity2, entity3, label, _ \
        = _populate_annotation_data(dbsession)

    res = _retrieve_entity_ids_and_annotation_values_by_user(
        dbsession=dbsession, users=[user1, user2, user3])
    assert res == {
        user1.id: [
            EntityAndAnnotationValuePair(entity1.id, 1),
            EntityAndAnnotationValuePair(entity2.id, 1),
            EntityAndAnnotationValuePair(entity3.id, -1)],
        user2.id: [
            EntityAndAnnotationValuePair(entity1.id, 1),
            EntityAndAnnotationValuePair(entity2.id, -1)],
        user3.id: [EntityAndAnnotationValuePair(entity3.id, -1)]}

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
        label_name=label.name)

    assert kappa_raw_data == {
        label.name: {
            tuple(sorted([user1.username, user2.username])): res1,
            tuple(sorted([user1.username, user3.username])): res2,
            tuple(sorted([user2.username, user3.username])): res3
        }
    }


def _populate_annotation_requests(dbsession):
    username1 = "ooo"
    username2 = "ppp"
    text1 = "whatever"
    text2 = "SQL is no fun."
    text3 = "Blahblah."
    taskname1 = "task1"
    taskname2 = "task2"
    default_params = "whatever"
    request_name = "name1"

    user1 = User(username=username1)
    user2 = User(username=username2)

    entity1 = Entity(name=text1, entity_type_id=1)
    entity2 = Entity(name=text2, entity_type_id=1)
    entity3 = Entity(name=text3, entity_type_id=1)

    task1 = Task(name=taskname1, default_params=default_params)
    task2 = Task(name=taskname2, default_params=default_params)

    dbsession.add_all([user1, user2, entity1, entity2,
                       entity3, task1, task2])
    dbsession.commit()

    # Requests for user 1
    request1 = AnnotationRequest(
        user_id=user1.id,
        entity_id=entity1.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id,
        name=request_name
    )

    request2 = AnnotationRequest(
        user_id=user1.id,
        entity_id=entity2.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Complete,
        task_id=task1.id
    )


    request3 = AnnotationRequest(
        user_id=user1.id,
        entity_id=entity3.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Stale,
        task_id=task1.id
    )

    # Requests for user 2
    request4 = AnnotationRequest(
        user_id=user2.id,
        entity_id=entity1.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id
    )

    request5 = AnnotationRequest(
        user_id=user2.id,
        entity_id=entity2.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task1.id
    )

    request6 = AnnotationRequest(
        user_id=user2.id,
        entity_id=entity3.id,
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


def test_fetch_ar_names(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    res = fetch_ar_names(dbsession=dbsession, task_id=task1.id,
                         username=user1.username)

    for request in requests:
        if request.task_id == task1.id and request.user.username == \
                user1.username:
            assert request.id in set(res)
        else:
            assert request.id not in set(res)


def test_fetch_annotated_ar_names_from_db(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    res = fetch_annotated_ar_names_from_db(dbsession=dbsession,
                                           task_id=task1.id,
                                           username=user1.username)

    for request in requests:
        if request.task_id == task1.id and request.user.username == \
                user1.username and request.status == \
                AnnotationRequestStatus.Complete:
            assert request.id in set(res)
        else:
            assert request.id not in set(res)


def test_fetch_ar_from_db(dbsession):
    task1, task2, user1, user2, requests = _populate_annotation_requests(
        dbsession)
    res = fetch_ar_by_name_from_db(dbsession, task_id=task1.id,
                                   user_id=user1.id, ar_name=requests[0].name)
    print(res)

    query = dbsession.query(Label.name, ClassificationAnnotation.value).\
        join(Label).filter(ClassificationAnnotation.id == 1)
    print(query)
    print(query.one_or_none())

    query2 = dbsession.query(AnnotationRequest.task_id, Task.name).distinct(
        AnnotationRequest.task_id, Task.name).join(Task).join(
        User).filter(User.username == "username")
    print(query2)

    # query = dbsession.query(AnnotationRequest).filter(
    #     AnnotationRequest.name == "name1")
    # print(query)
    # print(query.one_or_none().name)
    #
    # query2 = dbsession.query(AnnotationRequest.name).join(User).filter(
    #     AnnotationRequest.task_id == task1.id,
    #     User.username == user1.username,
    #     AnnotationRequest.id > requests[0].id).order_by(
    #     AnnotationRequest.id.asc())
    #
    # print(query2.first())

