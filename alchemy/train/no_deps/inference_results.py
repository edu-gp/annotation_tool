import re
from typing import Optional

from .utils import raw_to_pos_prob, save_file_numpy, load_file_numpy


class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = raw_to_pos_prob(self.raw)

    def save(self, inf_fname):
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"
        save_file_numpy(inf_fname, self.raw, type='local')

    @staticmethod
    def load(inf_fname) -> Optional["InferenceResults"]:
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"

        try:
            loaded_raw = load_file_numpy(inf_fname, type='local', numpy_kwargs=dict(allow_pickle=True))
        except OSError as e:
            if re.match(".*Failed to interpret .* as a pickle.*", str(e)):
                # Occurs when the file is an invalid pickle.
                return None
            else:
                raise
        else:
            return InferenceResults(loaded_raw)
