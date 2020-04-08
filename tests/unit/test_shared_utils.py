from shared.utils import (
    stem
)


def test_stem():
    assert stem('/foo/bar/my_file.json.gz') == 'my_file'
    assert stem('/blah/my_file.json.gz') == 'my_file'
    assert stem('my_file.json.gz') == 'my_file'
    assert stem('my_file.csv') == 'my_file'
    assert stem('my_file') == 'my_file'
