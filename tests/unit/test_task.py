import os
import tempfile
from db.task import Task, _convert_to_spacy_patterns
from shared.utils import save_jsonl

PATTERNS = [
    'Hello',
    'World'
]

CONVERTED_PATTERNS = [
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "hello"}]},
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "world"}]}
]


def test_convert_patterns():
    assert _convert_to_spacy_patterns(PATTERNS) == CONVERTED_PATTERNS


def _create_task():
    return Task.from_json({
        'task_id': 'testing123',
        'data_filenames': ['/blah.csv'],
        'annotators': ['eddie'],
        'labels': ['HEALTHCARE'],
        'patterns': PATTERNS
    })


def _create_task_no_patterns():
    return Task.from_json({
        'task_id': 'testing123',
        'data_filenames': ['/blah.csv'],
        'annotators': ['eddie'],
        'labels': ['HEALTHCARE'],
    })

# --- BASICS ---


def test_can_edit_label():
    task = _create_task()

    # Can add labels
    task.update(labels=['B', 'A'])
    # Labels are sorted
    assert task.labels == ['A', 'B']

    # Can remove labels
    task.update(labels=['A'])
    assert task.labels == ['A']

# --- PATTERNS ---


def test_patterns():
    task = _create_task()
    assert task.patterns == PATTERNS


def test_save_load_patterns():
    task = _create_task()
    data = task.to_json()
    loaded_task = Task.from_json(data)
    assert loaded_task.patterns == PATTERNS


def test_update_patterns():
    task = _create_task()
    new_patterns = PATTERNS[:1]
    task.update(patterns=new_patterns)
    assert task.patterns == new_patterns


def test_get_patterns_model():
    task = _create_task()
    pm = task.get_pattern_model()
    assert pm.predict(['hello there'], fancy=True) == [
        {'matches': [(0, 1, 'hello')], 'score': 0.5, 'tokens': ['hello', 'there']}]


def test_get_patterns_model_when_no_patterns():
    task = _create_task_no_patterns()
    pm = task.get_pattern_model()
    assert pm is None


def test_task_with_pattern_file():
    with tempfile.TemporaryDirectory() as tmpdirname:
        fname = os.path.join(tmpdirname, 'patterns.jsonl')
        save_jsonl(fname, [
            {"label": "HEALTHCARE", "pattern": [{"lower": "health"}]},
        ])

        more_patterns = [
            "healthy"
        ]

        task = Task.from_json({
            'task_id': 'testing123',
            'data_filenames': ['/blah.csv'],
            'annotators': ['eddie'],
            'labels': ['HEALTHCARE'],
            'patterns_file': fname,
            'patterns': more_patterns
        })

        model = task.get_pattern_model()

        preds = model.predict(['my dog is healthy'], fancy=True)
        assert preds == [{'tokens': ['my', 'dog', 'is', 'healthy'],
                          'matches': [(3, 4, 'healthy')], 'score': 0.25}]

        preds = model.predict(['my health is great'], fancy=True)
        assert preds == [{'tokens': ['my', 'health', 'is', 'great'], 'matches': [
            (1, 2, 'health')], 'score': 0.25}]
