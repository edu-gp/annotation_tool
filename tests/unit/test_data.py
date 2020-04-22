import math

from tests.sqlalchemy_conftest import *
from ar.data import _compute_kappa_matrix, _compute_total_annotations, \
    _exclude_unknowns_for_kappa_calculation, \
    _retrieve_annotation_with_same_context_shared_by_two_users, \
    _construct_kappa_stats_raw_data, \
    _retrieve_context_ids_and_annotation_values_by_user, \
    ContextAndAnnotationValuePair
from db.model import User, ClassificationAnnotation, Label, Context
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

    populate_annotations()
    res = _compute_total_annotations(
        dbsession=dbsession, label_name="whatever"
    )
    assert len(res) == 2
    for num, name in res:
        if name == "ooo":
            assert num == 3
        elif name == "ppp":
            assert num == 1


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
        return user1, user2, user3

    user1, user2, user3 = populate_annotations()

    res = _retrieve_context_ids_and_annotation_values_by_user(dbsession,
                                                              [user1, user2,
                                                               user3])
    print(res)

    assert res == {
        1: [
            ContextAndAnnotationValuePair(1, 1),
            ContextAndAnnotationValuePair(2, 1),
            ContextAndAnnotationValuePair(3, -1)],
        2: [
            ContextAndAnnotationValuePair(1, 1),
            ContextAndAnnotationValuePair(2, -1)],
        3: [ContextAndAnnotationValuePair(3, -1)]}

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
