import uuid
from typing import List
import spacy
from spacy.matcher import Matcher
from .base import ITextCatModel

class PatternModel(ITextCatModel):
    def __init__(self, patterns, model_id=None):
        if model_id:
            self.model_id = model_id
        else:
            self.model_id = str(uuid.uuid4())
        
        self.patterns = patterns

        self._loaded = False

    def _load():
        if not self._loaded:
            nlp = spacy.load("en_core_web_sm")
            matcher = Matcher(nlp.vocab)

            for row in patterns:
                matcher.add(row['label'], None, row['pattern'])
                
            self.matcher = matcher
            self.nlp = nlp
            
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
            'patterns': self.patterns
        }

    @staticmethod
    def from_json(data):
        return PatternModel(data['patterns'], model_id=data['model_id'])
