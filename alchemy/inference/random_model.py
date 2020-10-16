from typing import List
import numpy as np
from .base import ITextCatModel


class RandomModel(ITextCatModel):
    def predict(self, text_list: List[str]) -> List:
        res = list(np.random.uniform(size=len(text_list)))
        res = [{'score': s} for s in res]
        return res
