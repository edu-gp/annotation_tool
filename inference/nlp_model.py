import hashlib
import numpy as np

from inference.base import ITextCatModel
from train.no_deps.utils import load_original_data_text
from train.no_deps.inference_results import InferenceResults
from train.no_deps.paths import _get_inference_fname
from train.paths import _get_version_dir
from db.model import Task


class NLPModel(ITextCatModel):
    # TODO exploit vs explore
    def __init__(self, dbsession, task_id):
        self.dbsession = dbsession
        self.task_id = task_id
        self._cache = None

    def __str__(self):
        # Note: This is also used as a cache key.
        return f"NLPModel v{self.version}"

    def _warm_up_cache(self):
        if self._cache is None:
            print("Warming up NLPModel cache...")

            self._cache = {}

            task = self.dbsession.query(Task).filter_by(
                id=self.task_id).one_or_none()

            model = task.get_active_nlp_model()

            for inf in model.file_inferences:
                if inf.input_filename in task.get_data_filenames():
                    df = inf.create_exported_dataframe(include_text=True)
                    df['hashed_text'] = df['text'].apply(_hash_text)
                    self._cache.update(
                        dict(zip(df['hashed_text'], df['probs'])))

        return self._cache

    def predict(self, text_list):
        self._warm_up_cache()

        res = []
        for text in text_list:
            prob = self._cache.get(_hash_text(text))
            if prob is None:
                # TODO run any inferences that have not been ran, instead of silently erroring out
                res.append({'score': 0., 'prob': None})
            else:
                res.append({'score': _get_uncertainty(prob), 'prob': prob})

        return res


def _hash_text(text):
    text = text or ''
    return hashlib.md5(text.strip().encode()).hexdigest()


def _get_uncertainty(pred, eps=1e-6):
    # Return entropy as the uncertainty value
    pred = np.array(pred)
    return float(-np.sum(pred * np.log(pred + eps)))
