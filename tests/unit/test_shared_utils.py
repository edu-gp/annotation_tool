from shared.utils import (
    stem,
    list_to_textarea, textarea_to_list,
    json_lookup,
    build_counter,
    get_entropy,
    get_majority_vote
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


def test_build_counter():
    res = build_counter([-1, -1, 0, 1, 1, float('nan'), None, 1])
    assert dict(res) == {1: 3, -1: 2}


def test_get_entropy():
    a = get_entropy([1, 1, 1, 1])
    b = get_entropy([-1, -1, -1, -1])

    c = get_entropy([-1, -1, 1, 1])
    d = get_entropy([-1, -1, 1, 1, None, float('nan'), float('nan')])

    e = get_entropy([-1, 1, 1, 1])

    assert a == b
    assert c == d
    assert c > a, "[-1, -1, 1, 1] has higher entropy than [1, 1, 1, 1]"
    assert c > e, "[-1, -1, 1, 1] has higher entropy than [-1, 1, 1, 1]"
    assert e > a, "[-1, 1, 1, 1] has higher entropy than [1, 1, 1, 1]"


def test_get_majority_vote():
    res = get_majority_vote([1, 1, 1, 1, -1, -1, None, float('nan')])
    assert res == 1

    res = get_majority_vote([1, 1, -1, -1, -1, -1, None, float('nan')])
    assert res == -1

    res = get_majority_vote([1, 1, 1, -1, -1, -1, None, float('nan')])
    assert res in [1, -1], "could be either 1 or -1"
