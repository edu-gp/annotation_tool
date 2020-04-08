from functools import cmp_to_key
from typing import List
import spacy
# from spacy.matcher import Matcher
from spacy.matcher import PhraseMatcher
from .base import ITextCatModel


def _compare_matches(item1, item2):
    """Compare two matches from the Pattern Matcher by match length.

    :param item1: first matched item
    :param item2: second matched item
    :return: comparison result where -1 means item1 < item2, 1 means item1 >
    item2 and 0 means item1 == item2
    """
    if (item1[2] - item1[1]) < (item2[2] - item2[1]):
        return -1
    elif (item1[2] - item1[1]) > (item2[2] - item2[1]):
        return 1
    else:
        return _compare_last_matching_index(item1, item2)


def _compare_last_matching_index(item1, item2):
    """Compare two matches from the Pattern Matcher by the last matching index.

    :param item1: first matches item
    :param item2: second matches item
    :return: comparison result where -1 means item1 < item2, 1 means item1 >
    item2 and 0 means item1 == item2
    """
    if item1[2] < item2[2]:
        return -1
    elif item1[2] > item2[2]:
        return 1
    else:
        return 0


class PatternModel(ITextCatModel):
    def __init__(self, spacy_patterns):
        """
        Inputs:
            spacy_patterns: A list of json patterns Spacy understands.
                e.g. [
                    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "hello"}]},
                    {"label": "POSITIVE_CLASS", "pattern": [{"lower": "world"}]}
                ]
        """
        self.spacy_patterns = spacy_patterns or []
        self._loaded = False

    def __str__(self):
        # Note: This is also used as a cache key.
        return f'PatternModel <{len(self.spacy_patterns)} patterns>'

    def _load(self):
        if not self._loaded:
            nlp = spacy.load("en_core_web_sm")
            # matcher = Matcher(nlp.vocab)
            matcher = PhraseMatcher(nlp.vocab)

            for row in self.spacy_patterns:
                # matcher.add(row['label'], None, row['pattern'])

                # TODO temporary fix:
                # Assuming it's of the form "pattern": [{"lower": "my phrase"}]
                if len(row['pattern']) == 1 and 'lower' in row['pattern'][0]:
                    matcher.add(row['label'], None, nlp(
                        row['pattern'][0]['lower'].lower()))
                else:
                    raise Exception(f"Cannot load pattern: {row['pattern']}")

            self.matcher = matcher
            self.nlp = nlp

            self._loaded = True

    def predict(self, text_list: List[str], fancy=False) -> List:
        """Predict the match and score given the provided patterns.

        :param text_list: a list of text
        :param fancy: whether to return token and matched text in addition to a
        matching score
        :return: the longest and last match
        """
        self._load()

        res = []

        text_list = ['' if x is None else x
                     for x in text_list]

        # TODO disable the right things for speed
        for doc in self.nlp.pipe(text_list, disable=["tagger", "parser"]):
            matches = self.matcher(doc)
            matches.sort(key=cmp_to_key(_compare_matches))
            # m[2] and m[1] are start and end of matches
            len_of_matches = [m[2] - m[1] for m in matches]
            scores = [length / len(doc) if len(doc) > 0 else 0.
                      for length in len_of_matches]
            scores.sort()

            # Here we extract the longest match only.
            matches = matches[-1:]
            if fancy:
                _matches = []
                for match_id, start, end in matches:
                    span = doc[start:end]
                    _matches.append((start, end, span.text))
                res.append({
                    'tokens': [str(x) for x in list(doc)],
                    'matches': _matches,
                    'score': scores[-1]
                })
            else:
                res.append({
                    'score': scores[-1]
                })

        return res
