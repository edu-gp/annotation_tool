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

if __name__ == '__main__':
    assert _get_uncertainty([0.5, 0.5]) > _get_uncertainty([0.4, 0.6])
    assert _get_uncertainty([0.3, 0.7]) > _get_uncertainty([0.9, 0.1])
    assert _get_uncertainty([0.3, 0.3, 0.4]) > _get_uncertainty([0.1, 0.1, 0.8])

    model = NLPModel('cdff2935-744c-45de-a9cf-bff4a9c6264f', 3)

    text = "Provider of a real-time search and analytics platform designed to analyze bulk databases for facilitating and automating business operations. The company's platform combines a dashboard that utilizes artificial intelligence and heuristics to analyze and integrate real-time and retrospective data, provide timely and secure information specifically tailored for custom business operations, lets its users perform real-time search, real time analytics and receive real-time notifications and insight generated reports, enabling organizations in need of real-time data and information from diverse sources to make key strategic or tactical time sensitive decisions by analyzing up-to-date information, thereby making decisions based on live information."
    print(model.predict([text]))
