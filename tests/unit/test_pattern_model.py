from functools import cmp_to_key

from alchemy.inference.pattern_model import PatternModel, \
    _maximize_non_overlapping_matches, _compare_matches

import pytest


def test_pattern_model():
    patterns = [
        {"label": "HEALTHCARE", "pattern": [{"lower": "health"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "healthy"}]},
    ]

    model = PatternModel(patterns)
    preds = model.predict(['my dog is healthy'], fancy=True)
    assert preds == [{'tokens': ['my', 'dog', 'is', 'healthy'],
                      'matches': [(3, 4, 'healthy')], 'score': 0.25}]


def test_pattern_model_phrase():
    patterns = [
        {"label": "HEALTHCARE", "pattern": [{"lower": "my"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "dog is healthy"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "and happy"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "healthy and happy"}]},
    ]

    model = PatternModel(patterns)
    preds = model.predict(['my dog is healthy and happy'], fancy=True)
    assert preds == [{'tokens': ['my', 'dog', 'is', 'healthy', 'and', 'happy'],
                      'matches': [(0, 1, 'my'), (3, 6, 'healthy and happy')],
                      'score': 4.0 / 6}]


@pytest.mark.parametrize(
    "matches,expected",
    [
        ([("", 3, 4), ("", 1, 2)], [("", 1, 2), ("", 3, 4)]),
        ([("", 3, 5), ("", 1, 2)], [("", 1, 2), ("", 3, 5)]),
        ([("", 3, 5), ("", 1, 6)], [("", 1, 6), ("", 3, 5)]),
        ([("", 3, 5), ("", 3, 6)], [("", 3, 5), ("", 3, 6)]),
        ([("", 3, 7), ("", 3, 5)], [("", 3, 5), ("", 3, 7)]),
        ([("", 3, 5), ("", 3, 5)], [("", 3, 5), ("", 3, 5)]),
    ])
def test_compare_matches(matches, expected):
    # format: ("", match_start_index, match_end_index_exclusive)
    sorted_matches = sorted(matches, key=cmp_to_key(_compare_matches))
    assert sorted_matches == expected


def test_maximize_non_overlapping_matches():
    # format: ("", match_start_index, match_end_index_exclusive)
    matches = [
        ("", 1, 2),
        ("", 2, 3),
        ("", 2, 4),
        ("", 3, 5),
        ("", 4, 5),
        ("", 4, 6),
        ("", 8, 9)
    ]
    selected_matches = _maximize_non_overlapping_matches(matches=matches)
    assert selected_matches == {
        ("", 1, 2),
        ("", 4, 6),
        ("", 8, 9)
    }

    matches2 = [
        ("", 1, 2),
        ("", 2, 3),
        ("", 2, 4),
        ("", 3, 4),
        ("", 3, 5),
        ("", 4, 5),
        ("", 4, 6),
    ]
    selected_matches = _maximize_non_overlapping_matches(matches=matches2)
    assert selected_matches == {
        ("", 1, 2),
        ("", 4, 6),
    }

    matches3 = []
    selected_matches = _maximize_non_overlapping_matches(matches=matches3)
    assert selected_matches == {}

    matches4 = [("", 1, 2),]
    selected_matches = _maximize_non_overlapping_matches(matches=matches4)
    assert selected_matches == {("", 1, 2)}
