from typing import List
import spacy
# from spacy.matcher import Matcher
from spacy.matcher import PhraseMatcher
from .base import ITextCatModel


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
            #matcher = Matcher(nlp.vocab)
            matcher = PhraseMatcher(nlp.vocab)

            for row in self.spacy_patterns:
                #matcher.add(row['label'], None, row['pattern'])

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
        self._load()

        res = []

        text_list = ['' if x is None else x
                     for x in text_list]

        # TODO disable the right things for speed
        for doc in self.nlp.pipe(text_list, disable=["tagger", "parser"]):
            matches = self.matcher(doc)

            # m[2] and m[1] are start and end of matches
            len_of_matches = [m[2] - m[1] for m in matches]
            score = sum(len_of_matches) / len(doc) if len(doc) > 0 else 0.

            if fancy:
                _matches = []
                for match_id, start, end in matches:
                    span = doc[start:end]
                    _matches.append((start, end, span.text))
                res.append({
                    'tokens': [str(x) for x in list(doc)],
                    'matches': _matches,
                    'score': score
                })
            else:
                res.append({
                    'score': score
                })

        return res
