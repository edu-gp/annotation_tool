from alchemy.db.utils import _is_data_file_of_type


def make_data_file(tmpdir):
    f = tmpdir.join("data.jsonl")
    f.write('{"text":"hello"}\n{"text":"world"}')
    return str(f)


def make_pattern_file(tmpdir):
    f = tmpdir.join("pattern.jsonl")
    f.write(
        '{"label": "LifeSciences", "pattern": [{"lower": "drug"}]}\n{"label": "LifeSciences", "pattern": [{"lower": "medicine"}]}'
    )
    return str(f)


def make_invalid_file(tmpdir):
    f = tmpdir.join("invalid.jsonl")
    f.write("blah\nblah")
    return str(f)


def test_file_type_detector(tmpdir):
    data_file = make_data_file(tmpdir)
    pattern_file = make_pattern_file(tmpdir)
    invalid_file = make_invalid_file(tmpdir)

    assert _is_data_file_of_type(data_file, file_type='text') is True
    assert _is_data_file_of_type(pattern_file, file_type='text') is False
    assert _is_data_file_of_type(invalid_file, file_type='text') is False

    assert _is_data_file_of_type(data_file, file_type='pattern') is False
    assert _is_data_file_of_type(pattern_file, file_type='pattern') is True
    assert _is_data_file_of_type(invalid_file, file_type='pattern') is False
