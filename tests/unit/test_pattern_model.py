import tempfile
import os
from inference.pattern_model import PatternModel
from shared.utils import save_jsonl

def test_pattern_model():
    with tempfile.TemporaryDirectory() as tmpdirname:
        fname = os.path.join(tmpdirname, 'patterns.jsonl')

        patterns = [
            {"label":"HEALTHCARE","pattern":[{"lower":"health"}]},
            {"label":"HEALTHCARE","pattern":[{"lower":"healthy"}]},
        ]

        save_jsonl(fname, patterns)

        model = PatternModel('my_task_id', fname)

        preds = model.predict(['my dog is healthy'], fancy=True)

        assert preds == [{'tokens': ['my', 'dog', 'is', 'healthy'], 'matches': [(3, 4, 'healthy')], 'score': 0.25}]
