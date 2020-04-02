from ar.data import _majority_label


def test_majority_label():
    assert _majority_label([]) is None
    assert _majority_label([0, 0]) is None
    assert _majority_label([1, 1, 0, 1]) == 1
    assert _majority_label([1, -1, 0, 1]) == 1
    assert _majority_label([-1, -1, 0, 1]) == -1
