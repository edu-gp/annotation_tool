from numpy import save, load
from .utils import raw_to_pos_prob


class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = raw_to_pos_prob(self.raw)

    def save(self, inf_fname):
        save(inf_fname + '.npy', self.raw)

    @staticmethod
    def load(inf_fname):
        return InferenceResults(load(inf_fname + '.npy', allow_pickle=True))
