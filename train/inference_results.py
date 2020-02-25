from numpy import save, load
from scipy.special import softmax
from .paths import _get_inference_fname

class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = softmax(self.raw, axis=1)

    def save(self, inf_fname):
        save(inf_fname + '.npy', self.raw)

    @staticmethod
    def load(inf_fname):
        return InferenceResults(load(inf_fname + '.npy'))

    @staticmethod
    def load_from_task_version_fname(task_id, version, datafname):
        inf_fname = _get_inference_fname(task_id, version, datafname)
        return InferenceResults.load(inf_fname)

    def get_prob_for_class(self, class_idx):
        return self.probs[:, class_idx]
