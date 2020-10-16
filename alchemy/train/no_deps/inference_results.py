import os
import re
from typing import Optional

from numpy import load, save

from .utils import raw_to_pos_prob


class InferenceResults:
    def __init__(self, raw):
        self.raw = raw
        self.probs = raw_to_pos_prob(self.raw)

    def save(self, inf_fname):
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"
        save(inf_fname, self.raw)

    @staticmethod
    def load(inf_fname) -> Optional["InferenceResults"]:
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"
        if os.path.isfile(inf_fname):
            try:
                loaded_raw = load(inf_fname, allow_pickle=True)
            except OSError as e:
                if re.match(".*Failed to interpret .* as a pickle.*", str(e)):
                    # Occurs when the file is an invalid pickle.
                    return None
                else:
                    raise
            else:
                return InferenceResults(loaded_raw)
        else:
            return None
