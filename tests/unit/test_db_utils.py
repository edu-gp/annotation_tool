from alchemy.db.utils import is_data_file, is_pattern_file


def make_data_file(tmpdir):
    f = tmpdir.join("data.jsonl")
    f.write('{"text":"hello"}\n{"text":"world"}')
    return str(f)


def make_pattern_file(tmpdir):
    f = tmpdir.join("pattern.jsonl")
    f.write(
        '{"label": "LifeSciences", "pattern": [{"lower": "drug"}]}\n{"label": "LifeSciences", "pattern": [{"lower": "medicine"}]}')
    return str(f)


def make_invalid_file(tmpdir):
    f = tmpdir.join("invalid.jsonl")
    f.write('blah\nblah')
    return str(f)


def test_file_type_detector(tmpdir):
    data_file = make_data_file(tmpdir)
    pattern_file = make_pattern_file(tmpdir)
    invalid_file = make_invalid_file(tmpdir)

    assert is_data_file(data_file) is True
    assert is_data_file(pattern_file) is False
    assert is_data_file(invalid_file) is False

    assert is_pattern_file(data_file) is False
    assert is_pattern_file(pattern_file) is True
    assert is_pattern_file(invalid_file) is False
