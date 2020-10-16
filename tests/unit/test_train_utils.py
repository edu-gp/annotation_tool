from alchemy.train.no_deps.utils import (
    _parse_labels, MULTILABEL_CLASSIFICATION, BINARY_CLASSIFICATION,
    raw_to_pos_prob,
)
import numpy as np


def test_parse_labels_multilabel():
    data = [
        {
            'labels': {
                'LABEL_1': 1,
                'LABEL_2': -1,
            }
        },
    ]

    assert _parse_labels(data) == (
        [[1, 0]], MULTILABEL_CLASSIFICATION, ['LABEL_1', 'LABEL_2'])


def test_parse_labels_one_binary():
    data = [
        {
            'labels': {
                'LABEL_1': 1,
            }
        },
        {
            'labels': {
                'LABEL_1': -1,
            }
        },
    ]

    assert _parse_labels(data) == ([1, 0], BINARY_CLASSIFICATION, ['LABEL_1'])


def test_parse_labels_drop_label():
    data = [
        {
            'labels': {
                'LABEL_1': 1,
            }
        },
        {
            'labels': {
                'LABEL_1': 0,
            }
        },
    ]

    assert _parse_labels(data) == ([1], BINARY_CLASSIFICATION, ['LABEL_1'])


def test_parse_labels_empty():
    data = []

    assert _parse_labels(data) == ([], None, [])


def test_raw_to_pos_prob__without_sliding_window():
    # Raw output has one logit for negative class and one for positive class.
    raw = [
        [0.05716006, -0.03408603], [0.06059326, -0.03420808]
    ]

    pos_probs = raw_to_pos_prob(raw)
    assert np.all(np.isclose(pos_probs, [0.4772043, 0.4763174]))


def test_raw_to_pos_prob__with_sliding_window():
    # Raw output has one logit for negative class and one for positive class
    # for __each__ window.
    raw = [
        [
            [0.08940002, -0.10726406]
        ],
        [
            [0.08914353, -0.10628892], [0.08914353, -0.10628892]
        ]
    ]

    pos_probs = raw_to_pos_prob(raw)
    assert np.all(np.isclose(pos_probs, [0.4509918, 0.4512968]))
