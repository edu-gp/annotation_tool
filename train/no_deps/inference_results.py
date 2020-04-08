from numpy import save, load
from .utils import raw_to_pos_prob


class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = raw_to_pos_prob(self.raw)

    def save(self, inf_fname):
        if not inf_fname.endswith('.npy'):
            inf_fname = inf_fname + '.npy'
        save(inf_fname, self.raw)

    @staticmethod
    def load(inf_fname):
        if not inf_fname.endswith('.npy'):
            inf_fname = inf_fname + '.npy'
        return InferenceResults(load(inf_fname, allow_pickle=True))
