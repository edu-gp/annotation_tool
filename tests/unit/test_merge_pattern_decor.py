from ar import _merge_pattern_decor_for_all_labels_recursive


def test_merge_pattern_decor():
    pattern_decor1 = [
        {'token': ['a', 'b', 'c'], 'matches': [(0, 1, 'a')], 'score': 0.09},
        {'token': ['b', 'e', 'd'], 'matches': [(0, 1, 'b'), (1, 2, 'e')],
         'score': 0.06},
    ]

    pattern_decor2 = [
        {'token': ['a', 'b', 'c'], 'matches': [(1, 2, 'b')], 'score': 0.06},
        {'token': ['b', 'e', 'd'], 'matches': [(1, 2, 'e'), (2, 3, 'd')],
         'score': 0.09},
    ]

    pattern_decors = [pattern_decor1, pattern_decor2]
    res = _merge_pattern_decor_for_all_labels_recursive(
        pattern_decor_for_all_labels=pattern_decors,
        low=0,
        high=len(pattern_decors) - 1
    )
    print(res)
    assert res == [
        {'token': ['a', 'b', 'c'],
         'matches': [(0, 1, 'a'), (1, 2, 'b')],
         'score': [0.09, 0.06]},
        {'token': ['b', 'e', 'd'],
         'matches': [(0, 1, 'b'), (1, 2, 'e'), (2, 3, 'd')],
         'score': [0.06, 0.09]}
    ]
