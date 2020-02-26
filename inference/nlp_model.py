import hashlib
import numpy as np

from inference.base import ITextCatModel
from train.utils import load_original_data_text
from train.inference_results import InferenceResults

class NLPModel(ITextCatModel):
    # TODO exploit vs explore
    def __init__(self, task_id, version):
        self.task_id = task_id
        self.version = version
        self._cache = None

    def __str__(self):
        return f"NLPModel v{self.version}"

    def _iter_files_and_inferred(self):
        from db.task import Task
        task = Task.fetch(self.task_id)
        for fname in task.get_full_data_fnames():

            text = load_original_data_text(fname)

            ir = InferenceResults.load_from_task_version_fname(
                    self.task_id, self.version, fname)
            probs = ir.probs

            # TODO run any inferences that have not been ran
            assert len(text) == len(probs), 'Mismatch orig and inferred length. Did original data change?'

            for t, p in zip(text, probs):
                yield (t, p)

    def _warm_up_cache(self):
        if self._cache is None:
            print("Warming up NLPModel cache...")
            self._cache = {}
            for text, pred in self._iter_files_and_inferred():
                self._cache[_hash_text(text)] = pred
        return self._cache

    def predict(self, text_list):
        self._warm_up_cache()

        res = []
        for text in text_list:
            pred = self._cache.get(_hash_text(text))
            if pred is None:
                # TODO run any inferences that have not been ran, instead of silently erroring out
                res.append({'score': 0.})
            else:
                res.append({'score': _get_uncertainty(pred)})

        return res

def _hash_text(text):
    text = text or ''
    return hashlib.md5(text.strip().encode()).hexdigest()

def _get_uncertainty(pred, eps=1e-6):
    # Return entropy as the uncertainty value
    pred = np.array(pred)
    return float(-np.sum(pred * np.log(pred + eps)))
