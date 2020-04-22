import math

from tests.sqlalchemy_conftest import *
from ar.data import _compute_kappa_matrix, _compute_total_annotations, \
    _exclude_unknowns_for_kappa_calculation, \
    _retrieve_annotation_with_same_context_shared_by_two_users, \
    _construct_kappa_stats_raw_data, \
    _retrieve_context_ids_and_annotation_values_by_user, \
    ContextAndAnnotationValuePair, compute_annotation_request_statistics
from db.model import User, ClassificationAnnotation, Label, Context, \
    AnnotationRequest, AnnotationType, AnnotationRequestStatus, Task
from shared.utils import generate_md5_hash


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


def test__compute_total_annotations(dbsession):
    def populate_annotations():
        user1 = User(username="ooo")
        user2 = User(username="ppp")
        label = Label(name="whatever")
        dbsession.add_all([user1, user2, label])
        dbsession.commit()
        annotation1 = ClassificationAnnotation(value=1, user_id=user1.id,
                                               label_id=label.id)
        annotation2 = ClassificationAnnotation(value=1, user_id=user1.id,
                                               label_id=label.id)
        annotation3 = ClassificationAnnotation(value=1, user_id=user1.id,
                                               label_id=label.id)
        annotation4 = ClassificationAnnotation(value=1, user_id=user2.id,
                                               label_id=label.id)
        dbsession.add_all([annotation1, annotation2, annotation3, annotation4])

        return user1, user2

    user1, user2 = populate_annotations()
    res = _compute_total_annotations(
        dbsession=dbsession, label_name="whatever"
    )

    print(_compute_total_annotations2(dbsession=dbsession,
                                      label_name="whatever"))
    for num, name, user_id in res:
        if name == "ooo":
            assert num == 3
            assert user_id == user1.id
        elif name == "ppp":
            assert num == 1
            assert user_id == user2.id


def test__construct_kappa_stats_raw_data(dbsession):
    username1 = "ooo"
    username2 = "ppp"
    username3 = "qqq"
    label_name = "whatever"
    text1 = "whatever"
    text2 = "SQL is no fun."
    text3 = "Blahblah."

    def populate_annotations():
        user1 = User(username=username1)
        user2 = User(username=username2)
        user3 = User(username=username3)
        label = Label(name=label_name)

        context1 = Context(data=text1, hash=generate_md5_hash(text1))
        context2 = Context(data=text2, hash=generate_md5_hash(text2))
        context3 = Context(data=text3, hash=generate_md5_hash(text3))

        dbsession.add_all([user1, user2, user3, label, context1, context2,
                           context3])
        dbsession.commit()

        # A1 and A2 from user1 has the same context with A4 and A5 from user2.
        # A3 from user1 has the same context with A6 from user3.
        annotation1 = ClassificationAnnotation(value=1, user_id=user1.id,
                                               label_id=label.id,
                                               context_id=context1.id)
        annotation2 = ClassificationAnnotation(value=1, user_id=user1.id,
                                               label_id=label.id,
                                               context_id=context2.id)
        annotation3 = ClassificationAnnotation(value=-1, user_id=user1.id,
                                               label_id=label.id,
                                               context_id=context3.id)

        annotation4 = ClassificationAnnotation(value=1, user_id=user2.id,
                                               label_id=label.id,
                                               context_id=context1.id)
        annotation5 = ClassificationAnnotation(value=-1, user_id=user2.id,
                                               label_id=label.id,
                                               context_id=context2.id)
        annotation6 = ClassificationAnnotation(value=-1, user_id=user3.id,
                                               label_id=label.id,
                                               context_id=context3.id)
        dbsession.add_all([annotation1, annotation2, annotation3,
                           annotation4, annotation5, annotation6])
        return user1, user2, user3, context1, context2, context3

    user1, user2, user3, context1, context2, context3 = populate_annotations()

    res = _retrieve_context_ids_and_annotation_values_by_user(dbsession,
                                                              [user1, user2,
                                                               user3])
    assert res == {
        user1.id: [
            ContextAndAnnotationValuePair(context1.id, 1),
            ContextAndAnnotationValuePair(context2.id, 1),
            ContextAndAnnotationValuePair(context3.id, -1)],
        user2.id: [
            ContextAndAnnotationValuePair(context1.id, 1),
            ContextAndAnnotationValuePair(context2.id, -1)],
        user3.id: [ContextAndAnnotationValuePair(context3.id, -1)]}

    res1 = _retrieve_annotation_with_same_context_shared_by_two_users(
        user1=user1, user2=user2, contexts_and_annotation_values_by_user=res
    )
    assert res1 == {
        username1: [1, 1],
        username2: [1, -1]
    }

    res2 = _retrieve_annotation_with_same_context_shared_by_two_users(
        user1=user1, user2=user3, contexts_and_annotation_values_by_user=res
    )
    assert res2 == {
        username1: [-1],
        username3: [-1]
    }

    res3 = _retrieve_annotation_with_same_context_shared_by_two_users(
        user1=user2, user2=user3, contexts_and_annotation_values_by_user=res
    )
    assert res3 is None

    kappa_raw_data = _construct_kappa_stats_raw_data(
        dbsession=dbsession, distinct_users={user1, user2, user3},
        label_name=label_name)

    assert kappa_raw_data == {
        label_name: {
            tuple(sorted([username1, username2])): res1,
            tuple(sorted([username1, username3])): res2,
            tuple(sorted([username2, username3])): res3
        }
    }


def test_compute_annotation_request_statistics(dbsession):
    username1 = "ooo"
    username2 = "ppp"
    text1 = "whatever"
    text2 = "SQL is no fun."
    text3 = "Blahblah."
    taskname1 = "task1"
    taskname2 = "task2"
    default_params = "whatever"

    def populate_annotation_requests():
        user1 = User(username=username1)
        user2 = User(username=username2)

        context1 = Context(data=text1, hash=generate_md5_hash(text1))
        context2 = Context(data=text2, hash=generate_md5_hash(text2))
        context3 = Context(data=text3, hash=generate_md5_hash(text3))

        task1 = Task(name=taskname1, default_params=default_params)
        task2 = Task(name=taskname2, default_params=default_params)

        dbsession.add_all([user1, user2, context1, context2,
                           context3, task1, task2])
        dbsession.commit()

        # Requests for user 1
        request1 = AnnotationRequest(
            user_id=user1.id,
            context_id=context1.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Pending,
            task_id=task1.id
        )

        request2 = AnnotationRequest(
            user_id=user1.id,
            context_id=context2.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Complete,
            task_id=task1.id
        )

        # Requests for user 2
        request3 = AnnotationRequest(
            user_id=user1.id,
            context_id=context3.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Stale,
            task_id=task1.id
        )

        request4 = AnnotationRequest(
            user_id=user2.id,
            context_id=context1.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Pending,
            task_id=task1.id
        )

        request5 = AnnotationRequest(
            user_id=user2.id,
            context_id=context2.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Pending,
            task_id=task1.id
        )

        request6 = AnnotationRequest(
            user_id=user2.id,
            context_id=context3.id,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Complete,
            task_id=task1.id
        )

        dbsession.add_all([request1, request2, request3, request4, request5,
                           request6])
        dbsession.commit()
        return task1, task2, user1, user2

    task1, task2, user1, user2 = populate_annotation_requests()

    res = compute_annotation_request_statistics(dbsession, task1.id)
    assert res['total_outstanding_requests'] == 3
    assert res['n_outstanding_requests_per_user'] == {
        user1.username: 1,
        user2.username: 2
    }
