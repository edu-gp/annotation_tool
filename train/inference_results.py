from numpy import save, load
from scipy.special import softmax

class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = softmax(self.raw, axis=1)

    def save(self, fname):
        save(fname + '.npy', self.raw)

    @staticmethod
    def load(fname):
        return InferenceResults(load(fname + '.npy'))

    def get_prob_for_class(self, class_idx):
        return self.probs[:, class_idx]
