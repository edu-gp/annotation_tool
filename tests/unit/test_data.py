from ar.data import _compute_kappa_matrix, \
    _calculate_per_label_kappa_stats_table
from ar.data import _construct_per_label_per_user_result
import ar
import pytest

from mockito import when2, unstub


def test_compute_kappa_matrix():
    raw_data = {
        "label1": {
            "user_id1": [1, -1, 1, 1, -1],
            "user_id2": [-1, 1, 1, -1, 1],
        },
        "label12": {
            "user_id1": [1, -1, 1, -1, 1],
            "user_id2": [1, -1, -1, 1, 1],
        },
    }
    user_ids = ["user_id1", "user_id2"]
    kappa_matrix = _compute_kappa_matrix(
        user_ids=user_ids,
        kappa_stats_raw_data=raw_data
    )
    assert (len(kappa_matrix) == len(raw_data))
    for label in kappa_matrix:
        matrix_per_label = kappa_matrix[label]
        assert (len(matrix_per_label) == len(user_ids))
        for user in matrix_per_label:
            assert (len(matrix_per_label[user]) == len(user_ids))
            assert (matrix_per_label[user][user] == 1)


def test_construct_per_label_per_user_result():
    task_id = "FakeId"
    user_ids = ["User1", "User2"]
    labels = ["HEALTHCARE", "B2C"]
    annotation_ids = ["annotation1", "annotation2"]
    annotations = [
        {
            "anno": {"labels": {"HEALTHCARE": -1, "B2C": 1}}
        },
        {
            "anno": {"labels": {"HEALTHCARE": 1, "B2C": -1}}
        },
        {
            "anno": {"labels": {"HEALTHCARE": 1, "B2C": 1}}
        },
        {
            "anno": {"labels": {"HEALTHCARE": -1, "B2C": -1}}
        }
    ]

    when2(ar.data.fetch_annotation, task_id, user_ids[0], annotation_ids[0]) \
        .thenReturn(annotations[0])
    when2(ar.data.fetch_annotation, task_id, user_ids[0], annotation_ids[1]) \
        .thenReturn(annotations[1])
    when2(ar.data.fetch_annotation, task_id, user_ids[1], annotation_ids[0]) \
        .thenReturn(annotations[2])
    when2(ar.data.fetch_annotation, task_id, user_ids[1], annotation_ids[1]) \
        .thenReturn(annotations[3])

    kappa_stats_raw_data = _construct_per_label_per_user_result(
        task_id,
        user_ids,
        annotation_ids
    )

    for label in labels:
        assert label in kappa_stats_raw_data
        for user in user_ids:
            assert user in kappa_stats_raw_data[label]
            if label == labels[0] and user == user_ids[0]:
                assert kappa_stats_raw_data[label][user] == [-1, 1]
            elif label == labels[0] and user == user_ids[1]:
                assert kappa_stats_raw_data[label][user] == [1, -1]
            elif label == labels[1] and user == user_ids[0]:
                assert kappa_stats_raw_data[label][user] == [1, -1]
            else:
                assert kappa_stats_raw_data[label][user] == [1, -1]

    unstub()


@pytest.mark.parametrize("task_id,user_ids,annotation_ids,expected",
                         [
                             ("FakeId", ["User1"], ["anno1", "anno2"],
                              ["There is only one user User1"]),
                             ("FakeId", ["User1", "User2"], [],
                              ["There are no annotations from any user yet."]),
                             ("FakeId", ["User1", "User2"], [{0}, {1}],
                              ['No overlapping annotations found among users.'])
                         ]
                         )
def test_calculate_per_label_kappa_stats_table_edge_cases(task_id, user_ids,
                                                          annotation_ids,
                                                          expected):
    kappa_matrix_html_tables = _calculate_per_label_kappa_stats_table(
        task_id,
        user_ids,
        annotation_ids
    )

    assert kappa_matrix_html_tables == expected

