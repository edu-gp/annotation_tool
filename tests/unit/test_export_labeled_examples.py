from an.export_annotations import (
    _majority_label, export_distinct_examples
)


def test_majority_label():
    assert _majority_label([]) is None
    assert _majority_label([0, 0]) is None
    assert _majority_label([1, 1, 0, 1]) == 1
    assert _majority_label([1, -1, 0, 1]) == 1
    assert _majority_label([-1, -1, 0, 1]) == -1


def test__export_distinct_examples__simple():
    AR_1 = {
        'ar_id': 'foo_1',
        'data': {'text': 'sometext_1'}
    }
    AR_2 = {
        'ar_id': 'foo_2',
        'data': {'text': 'sometext_2'}
    }

    sample_annotations = [
        # User A annotates AR_1
        {
            'req': AR_1,
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                    'POP_HEALTH': -1,

                    # This one will be dropped since user is unsure
                    'AI': 0,
                }
            }
        },
        # User B annotates AR_1
        {
            'req': AR_1,
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                }
            }
        },
        # User C annotates AR_1
        {
            'req': AR_1,
            'anno': {
                'labels': {
                    # User C disagrees with User A and B, but they're the majority.
                    # So HEALTHCARE would be 1.
                    'HEALTHCARE': -1,
                }
            }
        },
        # User C annotates AR_2
        {
            'req': AR_2,
            'anno': {
                'labels': {
                    'HEALTHCARE': -1,
                }
            }
        },
    ]

    result = export_distinct_examples(sample_annotations)
    assert result == [
        {'text': 'sometext_1', 'labels': {'HEALTHCARE': 1, 'POP_HEALTH': -1}},
        {'text': 'sometext_2', 'labels': {'HEALTHCARE': -1}}
    ]


def test__export_distinct_examples__empty_text():
    """Currently we allow empty or missing text"""
    AR_1 = {
        'ar_id': 'bad_1',
        'data': {'text': None}
    }
    AR_2 = {
        'ar_id': 'bad_2',
        'data': {}
    }

    sample_annotations = [
        # User A annotates AR_1
        {
            'req': AR_1,
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                }
            }
        },
        # User B annotates AR_1
        {
            'req': AR_2,
            'anno': {
                'labels': {
                    'HEALTHCARE': 1,
                }
            }
        },
    ]

    result = export_distinct_examples(sample_annotations)

    # Note the behaviour for missing text is not ideal, but this is the
    # 'official' behaviour for now.
    assert result == [
        {'text': '', 'labels': {'HEALTHCARE': 1}},
        {'text': '', 'labels': {'HEALTHCARE': 1}}
    ]
