from alchemy.db._task import _convert_to_spacy_patterns, _Task

PATTERNS = ["Hello", "World"]

CONVERTED_PATTERNS = [
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "hello"}]},
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "world"}]},
]


def test_convert_patterns():
    assert _convert_to_spacy_patterns(PATTERNS) == CONVERTED_PATTERNS


def _create_task():
    return _Task.from_json(
        {
            "task_id": "testing123",
            "data_filenames": ["/blah.csv"],
            "annotators": ["eddie"],
            "labels": ["HEALTHCARE"],
            "patterns": PATTERNS,
        }
    )


def _create_task_no_patterns():
    return _Task.from_json(
        {
            "task_id": "testing123",
            "data_filenames": ["/blah.csv"],
            "annotators": ["eddie"],
            "labels": ["HEALTHCARE"],
        }
    )


# --- PATTERNS ---


def test_patterns():
    task = _create_task()
    assert task.patterns == PATTERNS


def test_save_load_patterns():
    task = _create_task()
    data = task.to_json()
    loaded_task = _Task.from_json(data)
    assert loaded_task.patterns == PATTERNS

