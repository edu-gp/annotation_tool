import re
from typing import Optional

from .utils import raw_to_pos_prob, save_file_numpy, load_file_numpy


class InferenceResults:
    def __init__(self, raw, data_store):
        self.raw = raw
        self.probs = raw_to_pos_prob(self.raw)
        self.data_store = data_store

    def save(self, inf_fname):
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"
        save_file_numpy(inf_fname, self.raw, data_store=self.data_store)

    @staticmethod
    def load(inf_fname, data_store) -> Optional["InferenceResults"]:
        if not inf_fname.endswith(".npy"):
            inf_fname = inf_fname + ".npy"

        try:
            loaded_raw = load_file_numpy(inf_fname, data_store=data_store, numpy_kwargs=dict(allow_pickle=True))
            if loaded_raw is None:
                return None
        except OSError as e:
            if re.match(".*Failed to interpret .* as a pickle.*", str(e)):
                # Occurs when the file is an invalid pickle.
                return None
            else:
                raise
        else:
            return InferenceResults(loaded_raw, data_store=data_store)
