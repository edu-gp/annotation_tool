from backend.annotations_utils import _parse_list, parse_form
import pytest


def test__parse_list():
    # No such key
    assert len(_parse_list({}, 'blah')) == 0

    # Invalid input
    assert len(_parse_list({'x': ['a', 'b']}, 'x')) == 0

    # Valid input
    assert _parse_list({'x': 'a\nb'}, 'x') == ['a', 'b']
    assert _parse_list({'x': 'a\r\n  b'}, 'x') == ['a', 'b']


def test_parse_form():
    form = {}
    with pytest.raises(Exception, match="User is required"):
        parse_form(form)

    form = {'user': 'a'}
    with pytest.raises(Exception, match="Label is required"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b'}
    user, label, domains, annotations = parse_form(form)
    assert user == 'a'
    assert label == 'b'
    assert len(domains) == 0
    assert len(annotations) == 0

    form = {'user': 'a', 'label': 'b', 'domains': 'a.com'}
    with pytest.raises(Exception, match=r"Number of domains .* does not match .* number of annotations .*"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b',
            'domains': 'a.com', 'annotations': '1\n-1'}
    with pytest.raises(Exception, match=r"Number of domains .* does not match .* number of annotations .*"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b',
            'domains': 'a.com\nb.com', 'annotations': '1\n-1'}
    user, label, domains, annotations = parse_form(form)
    assert user == 'a'
    assert label == 'b'
    assert len(domains) == 2
    assert len(annotations) == 2

    form = {'user': 'a', 'label': 'b',
            'domains': 'a.com\nb.com', 'annotations': '1\n5'}
    with pytest.raises(Exception, match=r"Annotation 5 is not in the list of acceptable annotations .*"):
        parse_form(form)
