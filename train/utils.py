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


import numpy as np
from scipy.special import softmax

def raw_to_pos_prob(raw):
    """Raw model output to positive class probability"""
    probs_pos_class = []
    for out in raw:
        out = np.array(out)
        if len(out.shape) == 1:
            # This is typical style of outputs.
            probs_pos_class.append(softmax(out)[1])
        elif len(out.shape) == 2:
            # This is the style of outputs when we use sliding windows.
            # Take the average prob of all the window predictions.
            _prob = softmax(out, axis=1)[:,1].mean()
            probs_pos_class.append(_prob)
        else:
            raise Exception(f"Unclear how to deal with raw dimension: {out.shape}")
    return probs_pos_class
