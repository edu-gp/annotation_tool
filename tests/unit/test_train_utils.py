from train.utils import _parse_labels, MULTILABEL_CLASSIFICATION, BINARY_CLASSIFICATION

def test_parse_labels_multilabel():
    data = [ 
        {
            'labels': {
                'LABEL_1': 1,
                'LABEL_2': -1,
            }
        },
    ]

    assert _parse_labels(data) == ([[1, 0]], MULTILABEL_CLASSIFICATION, ['LABEL_1', 'LABEL_2'])


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
