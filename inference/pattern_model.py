import uuid
from typing import List
import spacy
from spacy.matcher import Matcher
from .base import ITextCatModel

from shared.utils import load_jsonl
from db import _data_dir
import os

class PatternModel(ITextCatModel):
    def __init__(self, patterns_file, model_id=None):
        if model_id:
            self.model_id = model_id
        else:
            self.model_id = str(uuid.uuid4())
        
        self.patterns_file = patterns_file

        self._loaded = False

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
