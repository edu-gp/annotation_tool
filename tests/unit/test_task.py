from alchemy.db.model import _convert_to_spacy_patterns

PATTERNS = ["Hello", "World"]

CONVERTED_PATTERNS = [
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "hello"}]},
    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "world"}]},
]


def test_convert_patterns():
    assert _convert_to_spacy_patterns(PATTERNS) == CONVERTED_PATTERNS
