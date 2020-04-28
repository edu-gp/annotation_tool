from shared.utils import (
    stem,
    list_to_textarea, textarea_to_list,
)


def test_stem():
    assert stem('/foo/bar/my_file.json.gz') == 'my_file'
    assert stem('/blah/my_file.json.gz') == 'my_file'
    assert stem('my_file.json.gz') == 'my_file'
    assert stem('my_file.csv') == 'my_file'
    assert stem('my_file') == 'my_file'


def test_list_to_textarea_and_back():
    ls = ['a', 'bb', 'ccc']
    assert list_to_textarea(ls) == 'a\nbb\nccc'
    assert textarea_to_list(list_to_textarea(ls)) == ls
