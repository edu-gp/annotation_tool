import os

import numpy as np

from alchemy.train.no_deps.inference_results import InferenceResults


class TestInferenceResultsSaveLoad:
    def test_inference_results_save_load(self, tmpdir):
        raw = [[0.05716006, -0.03408603], [0.06059326, -0.03420808]]

        res = InferenceResults(raw)
        fname = os.path.join(tmpdir, "blah.npy")

        res.save(fname)

        res = InferenceResults.load(fname)

        for i in range(len(raw)):
            assert np.isclose(res.raw[i], raw[i]).all()

    def test_inference_results_save_load__sliding_window(self, tmpdir):
        raw = [
            [[0.08940002, -0.10726406]],
            [[0.08914353, -0.10628892], [0.08914353, -0.10628892]],
        ]

        res = InferenceResults(raw)
        fname = os.path.join(tmpdir, "blah.npy")

        res.save(fname)

        res = InferenceResults.load(fname)

        for i in range(len(raw)):
            assert np.isclose(res.raw[i], raw[i]).all()
