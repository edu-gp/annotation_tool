import os

import numpy as np

from alchemy.train.no_deps.inference_results import InferenceResults


class TestInferenceResultsSaveLoad:
    def test_inference_results_save_load(self, monkeypatch, tmpdir):
        data_store = 'local'
        monkeypatch.setenv("STORAGE_BACKEND", data_store)
        if data_store == 'cloud':
            tmpdir = '__filestore'
        monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmpdir))
        raw = [[0.05716006, -0.03408603], [0.06059326, -0.03420808]]

        res = InferenceResults(raw, data_store=data_store)
        fname = os.path.join(tmpdir, "blah.npy")

        res.save(fname)

        res = InferenceResults.load(fname, data_store=data_store)

        for i in range(len(raw)):
            assert np.isclose(res.raw[i], raw[i]).all()

    def test_inference_results_save_load__sliding_window(self, monkeypatch, tmpdir):
        data_store = 'local'
        monkeypatch.setenv("STORAGE_BACKEND", data_store)
        if data_store == 'cloud':
            tmpdir = '__filestore'
        monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmpdir))
        raw = [
            [[0.08940002, -0.10726406]],
            [[0.08914353, -0.10628892], [0.08914353, -0.10628892]],
        ]

        res = InferenceResults(raw, data_store=data_store)
        fname = os.path.join(tmpdir, "blah.npy")

        res.save(fname)

        res = InferenceResults.load(fname, data_store=data_store)

        for i in range(len(raw)):
            assert np.isclose(res.raw[i], raw[i]).all()
