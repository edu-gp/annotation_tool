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


def test_pattern_model_phrase():
    patterns = [
        {"label": "HEALTHCARE", "pattern": [{"lower": "is healthy"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "dog"}]},
        {"label": "HEALTHCARE", "pattern": [{"lower": "dog is"}]},
    ]

    model = PatternModel(patterns)

    preds = model.predict(['my dog is healthy'], fancy=False)
    assert preds == [{'score': 0.5}]

    preds_fancy = model.predict(['my dog is healthy'], fancy=True)
    assert preds_fancy == [{'tokens': ['my', 'dog', 'is', 'healthy'],
                            'matches': [(2, 4, 'is healthy')], 'score': 0.5}]
