from collections import Counter, defaultdict


def _majority_label(labels):
    '''
    Get the majority of non-zero labels
    Input: [1,1,0,0,0,0,-1,-1,1,1]
    Output: 1
    '''
    labels = [x for x in labels if x != 0]
    if len(labels) > 0:
        return Counter(labels).most_common()[0][0]
    else:
        return None


def export_distinct_examples(annotations_iterator):
    """
    Inputs:
        annotations_iterator: A iterator that returns annotations.

    An example annotation looks like:
        {
            'req': {
                'ar_id': '...'
                'data': {
                    'text': '...'
                }
            },
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                    'POP_HEALTH': -1,
                    'AI': 0,
                }
            }
        }

    Step 1. Labels will start off being gathered for each ar_id:
    {
        'ar_id_12345': {
            'HEALTHCARE': [1, 1, -1, 1, 0, 0]
        },
        ...
    }

    Step 2. Then they're merged by a merging strategy (currently majority vote):
    {
        'ar_id_12345': {
            'HEALTHCARE': 1
        },
        ...
    }

    Step 3. Finally we join labels with the text using ar_id.
    [
        {
            'text': '...',
            'labels': {'HEALTHCARE': 1}
        },
        ...
    ]

    This function returns result from the last step.
    """

    text = {}
    labels = defaultdict(lambda: defaultdict(list))

    # Step 1. Gather all the labels (and text)
    for anno in annotations_iterator:
        ar_id = anno['req']['ar_id']

        text[ar_id] = anno['req']['data'].get('text') or ''

        for label_key, label_value in anno['anno']['labels'].items():
            # e.g. label_key = 'HEALTHCARE', label_value = 1
            labels[ar_id][label_key].append(label_value)

    # Step 2. Merge all labels within each ar_id
    new_labels = {}
    for ar_id in labels:
        new_labels[ar_id] = {
            label_key: _majority_label(list_of_label_values)
            for label_key, list_of_label_values in labels[ar_id].items()
            if _majority_label(list_of_label_values) is not None
        }
    labels = new_labels

    # Step 3. Join with text on ar_id
    final = []
    for ar_id in labels:
        # If any labels are left - An example could have the "unsure" label for
        # all its annotations, and since we remove all unsure labels, there
        # might be any labels left.
        if len(labels[ar_id]) > 0:
            final.append({
                'text': text[ar_id],
                'labels': labels[ar_id]
            })

    return final
