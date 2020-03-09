from typing import List
import spacy
from spacy.matcher import Matcher
from .base import ITextCatModel

from shared.utils import load_jsonl
from db import _data_dir
import os

class PatternModel(ITextCatModel):
    # TODO does it make sense for PatternModel to require a task_id?
    def __init__(self, task_id, patterns_file):
        self.model_id = f'patterns-{task_id}'
        self.patterns_file = patterns_file
        self._loaded = False

    def __str__(self):
        return f'PatternModel <{self.patterns_file}>'

    def _load_patterns(self):
        return load_jsonl(os.path.join(_data_dir(), self.patterns_file), to_df=False)

    def _load(self):
        if not self._loaded:
            nlp = spacy.load("en_core_web_sm")
            matcher = Matcher(nlp.vocab)

            patterns = self._load_patterns()

            for row in patterns:
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

    def to_json(self):
        return {
            'type': 'PatternModel',
            'model_id': self.model_id,
            'patterns_file': self.patterns_file
        }

    @staticmethod
    def from_json(data):
        return PatternModel(data['patterns_file'], model_id=data['model_id'])
