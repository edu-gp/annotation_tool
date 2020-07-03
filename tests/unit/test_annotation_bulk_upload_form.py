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
    with pytest.raises(Exception, match="Entity type is required"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b', 'entity_type': 'company'}
    user, label, domains, annotations, entity_type = parse_form(form)
    assert user == 'a'
    assert label == 'b'
    assert len(domains) == 0
    assert len(annotations) == 0
    assert entity_type == 'company'

    form = {'user': 'a', 'label': 'b', 'entities': 'a.com', 'entity_type': 'company'}
    with pytest.raises(Exception, match=r"Number of entities .* does not match .* number of annotations .*"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b',
            'entities': 'a.com', 'annotations': '1\n-1', 'entity_type': 'company'}
    with pytest.raises(Exception, match=r"Number of entities .* does not match .* number of annotations .*"):
        parse_form(form)

    form = {'user': 'a', 'label': 'b',
            'entities': 'a.com\nb.com', 'annotations': '1\n-1', 'entity_type': 'company'}
    user, label, domains, annotations, entity_type = parse_form(form)
    assert user == 'a'
    assert label == 'b'
    assert len(domains) == 2
    assert len(annotations) == 2
    assert entity_type == 'company'

    form = {'user': 'a', 'label': 'b',
            'entities': 'a.com\nb.com', 'annotations': '1\n5', 'entity_type': 'company'}
    with pytest.raises(Exception, match=r"Annotation 5 is not in the list of acceptable annotations .*"):
        parse_form(form)
