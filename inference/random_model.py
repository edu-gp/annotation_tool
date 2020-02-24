from typing import List
import numpy as np
import uuid
from .base import ITextCatModel

class RandomModel(ITextCatModel):
    def __init__(self):
        # TODO no use for model_id for RandomModel, right?
        self.model_id = str(uuid.uuid4())

    def predict(self, text_list:List[str]) -> List:
        res = list(np.random.uniform(size=len(text_list)))
        res = [{'score': s} for s in res]
        return res
