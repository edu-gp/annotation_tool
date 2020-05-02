from shared.utils import (
    stem,
    list_to_textarea, textarea_to_list,
    json_lookup
)


def test_stem():
    assert stem('/foo/bar/my_file.json.gz') == 'my_file'
    assert stem('/blah/my_file.json.gz') == 'my_file'
    assert stem('my_file.json.gz') == 'my_file'
    assert stem('my_file.csv') == 'my_file'
    assert stem('my_file') == 'my_file'

    assert stem('/foo/bar/my_file.json.gz', include_suffix=True) \
        == 'my_file.json.gz'
    assert stem('/blah/my_file.json.gz', include_suffix=True) \
        == 'my_file.json.gz'
    assert stem('my_file.json.gz', include_suffix=True) == 'my_file.json.gz'
    assert stem('my_file.csv', include_suffix=True) == 'my_file.csv'
    assert stem('my_file', include_suffix=True) == 'my_file'


def test_list_to_textarea_and_back():
    ls = ['a', 'bb', 'ccc']
    assert list_to_textarea(ls) == 'a\nbb\nccc'
    assert textarea_to_list(list_to_textarea(ls)) == ls


def test_json_lookup():
    data = {
        'blah': {
            'foo': 12,
            'bar': {
                'x': 24
            }
        }
    }

    assert json_lookup(data, 'blah.foo') == 12
    assert json_lookup(data, 'blah.bar.x') == 24
    assert json_lookup(data, 'blah.foo.x') is None
    assert json_lookup(data, 'dne') is None
