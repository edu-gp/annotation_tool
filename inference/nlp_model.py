import hashlib
import numpy as np

from inference.base import ITextCatModel
from train.no_deps.utils import load_original_data_text
from train.no_deps.inference_results import InferenceResults
from train.no_deps.paths import _get_inference_fname
from train.paths import _get_version_dir


class NLPModel(ITextCatModel):
    # TODO exploit vs explore
    def __init__(self, task_id, version):
        self.task_id = task_id
        self.version = version
        self._cache = None

    def __str__(self):
        # Note: This is also used as a cache key.
        return f"NLPModel v{self.version}"

    def _iter_files_and_inferred(self):
        from db.task import Task
        task = Task.fetch(self.task_id)
        for fname in task.get_full_data_fnames():

            text = load_original_data_text(fname)

            version_dir = _get_version_dir(self.task_id, self.version)
            inf_fname = _get_inference_fname(version_dir, fname)
            ir = InferenceResults.load(inf_fname)

            probs = ir.probs

            # TODO run any inferences that have not been ran
            assert len(text) == len(
                probs), 'Mismatch orig and inferred length. Did original data change?'

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
                res.append({'score': 0., 'prob': None})
            else:
                res.append({'score': _get_uncertainty(pred), 'prob': pred})

        return res


def _hash_text(text):
    text = text or ''
    return hashlib.md5(text.strip().encode()).hexdigest()


def _get_uncertainty(pred, eps=1e-6):
    # Return entropy as the uncertainty value
    pred = np.array(pred)
    return float(-np.sum(pred * np.log(pred + eps)))
