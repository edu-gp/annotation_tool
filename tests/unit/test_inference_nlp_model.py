from inference.nlp_model import _get_uncertainty


def test_get_uncertainty():
    assert _get_uncertainty([0.5, 0.5]) > _get_uncertainty([0.4, 0.6])
    assert _get_uncertainty([0.3, 0.7]) > _get_uncertainty([0.9, 0.1])
    assert _get_uncertainty([0.3, 0.3, 0.4]) > \
        _get_uncertainty([0.1, 0.1, 0.8])
