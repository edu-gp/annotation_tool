from typing import List
import spacy
from spacy.matcher import Matcher
from .base import ITextCatModel

from shared.utils import load_jsonl
from db import _data_dir
import os

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
        return f'PatternModel <{len(self.spacy_patterns)} patterns>'

    def _load(self):
        if not self._loaded:
            nlp = spacy.load("en_core_web_sm")
            matcher = Matcher(nlp.vocab)

            for row in self.spacy_patterns:
                matcher.add(row['label'], None, row['pattern'])

            self.matcher = matcher
            self.nlp = nlp

            self._loaded = True

    def predict(self, text_list:List[str], fancy=False) -> List:
        self._load()

        res = []

        text_list = ['' if x is None else x
                     for x in text_list]

        # TODO disable the right things for speed
        for doc in self.nlp.pipe(text_list, disable=["tagger", "parser"]):
            matches = self.matcher(doc)
            score = len(matches) / len(doc) if len(doc) > 0 else 0.

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
