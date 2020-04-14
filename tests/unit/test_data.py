import math

import pytest
from mockito import when2, unstub

from ar.data import _compute_kappa_matrix, \
    _calculate_per_label_kappa_stats_table, \
    _exclude_unknowns_for_kappa_calculation
from ar.data import _construct_per_label_per_user_pair_result


def test__construct_per_label_per_user_pair_result():
    task_id = "fakeid"
    user_ids = ["user1", "user2", "user3"]

    anno_ids_per_user = {
        "user1": {"a1", "a2", "a3"},
        "user2": {"a1", "a3"},
        "user3": {"a2", "a3"}
    }

    results_per_task_user_anno_id = {
        (task_id, "user1", "a1"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                       "B2C": -1}}},
        (task_id, "user1", "a2"): {"anno": {"labels": {"HEALTHCARE": -1,
                                                       "B2C": 1}}},
        (task_id, "user1", "a3"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                       "B2C": -1}}},
        (task_id, "user2", "a1"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                      "B2C": -1}}},
        (task_id, "user2", "a2"): {"anno": {"labels": {"HEALTHCARE": -1,
                                                       "B2C": 1}}},
        (task_id, "user2", "a3"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                       "B2C": -1}}},
        (task_id, "user3", "a1"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                       "B2C": -1}}},
        (task_id, "user3", "a2"): {"anno": {"labels": {"HEALTHCARE": -1,
                                                       "B2C": 1}}},
        (task_id, "user3", "a3"): {"anno": {"labels": {"HEALTHCARE": 1,
                                                       "B2C": -1}}},
    }

    expected = {
        "HEALTHCARE": {
            ("user1", "user2"): {
                "user1": [1, 1],
                "user2": [1, 1]
            },
            ("user1", "user3"): {
                "user1": [-1, 1],
                "user3": [-1, 1]
            },
            ("user2", "user3"): {
                "user2": [1],
                "user3": [1]
            }
        },
        "B2C": {
            ("user1", "user2"): {
                "user1": [-1, -1],
                "user2": [-1, -1]
            },
            ("user1", "user3"): {
                "user1": [1, -1],
                "user3": [1, -1]
            },
            ("user2", "user3"): {
                "user2": [-1],
                "user3": [-1]
            }
        }
    }

    kappa_stats_raw_data = _construct_per_label_per_user_pair_result(
        task_id=task_id, user_ids=user_ids,
        annos_per_user=anno_ids_per_user,
        results_per_task_user_anno_id=results_per_task_user_anno_id
    )

    assert kappa_stats_raw_data == expected


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



@pytest.mark.parametrize("task_id,user_ids,annotations_per_user,"
                         "results_lookup,expected",
                         [
                             ("FakeId", ["User1"], {"User1": ["anno1",
                                                              "anno2"]}, {},
                              ["There is only one user User1"]),
                             ("FakeId", ["User1", "User2"], {}, {},
                              ["There are no annotations from any user yet."]),
                         ]
                         )
def test__calculate_per_label_kappa_stats_table_edge_cases(task_id, user_ids,
                                                          annotations_per_user,
                                                          results_lookup,
                                                          expected):
    kappa_matrix_html_tables = _calculate_per_label_kappa_stats_table(
        task_id,
        user_ids,
        annotations_per_user,
        results_lookup
    )

    assert kappa_matrix_html_tables == expected
