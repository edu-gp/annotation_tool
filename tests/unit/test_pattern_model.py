from inference.pattern_model import PatternModel


def test_pattern_model():
    patterns = [
        {"label": "HEALTHCARE", "pattern": [{"lower": "health"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "healthy"}]},
    ]

    model = PatternModel(patterns)
    preds = model.predict(['my dog is healthy'], fancy=True)
    assert preds == [{'tokens': ['my', 'dog', 'is', 'healthy'],
                      'matches': [(3, 4, 'healthy')], 'score': 0.25}]
