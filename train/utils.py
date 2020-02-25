from shared.utils import load_jsonl

def load_original_data_text(datafname):
    text = load_jsonl(datafname)['text']
    text = text.fillna('')
    text = list(text)
    return text



BINARY_CLASSIFICATION = 'binary'
MULTILABEL_CLASSIFICATION = 'multilabel'

def _parse_labels(data):
    '''
    Inputs:
        data: [ 
            {
                'labels': {
                    'LABEL_1': 1,
                    'LABEL_2': -1,
                }
                ...
            },
            ...
        ]
    '''

    y = []
    problem_type = None
    class_order = []

    all_labels = set()
    for row in data:
        for label in row['labels']:
            all_labels.add(label)

    class_order = sorted(list(all_labels))

    if len(class_order) > 0:

        if len(class_order) == 1:
            problem_type = BINARY_CLASSIFICATION

            y = []

            for row in data:
                val = list(row['labels'].values())[0]
                if val == 1:
                    y.append(1)
                elif val == -1:
                    y.append(0)

        else:
            problem_type = MULTILABEL_CLASSIFICATION

            y = []

            class_order_idx = {k: i for i, k in enumerate(class_order)}

            for row in data:
                _y = [0] * len(class_order)
                for label in row['labels']:
                    if row['labels'][label] == 1:
                        _y[class_order_idx[label]] = 1
                y.append(_y)

    return y, problem_type, class_order


if __name__ == '__main__':
    data = [ 
        {
            'labels': {
                'LABEL_1': 1,
                'LABEL_2': -1,
            }
        },
    ]

    assert _parse_labels(data) == ([[1, 0]], MULTILABEL_CLASSIFICATION, ['LABEL_1', 'LABEL_2'])


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


    data = []

    assert _parse_labels(data) == ([], None, [])

