from alchemy.train.no_deps.inference_results import InferenceResults


def test_train_flow(monkeypatch, tmp_path):
    data_store = 'local'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    p = tmp_path / "valid.npy"
    InferenceResults([[20, 20], [-10, 20]], data_store=data_store).save(str(p))

    inf = InferenceResults.load(str(p), data_store=data_store)
    assert inf is not None, "Should be able to load this no problem."

    p = tmp_path / "does_not_exist.npy"
    inf = InferenceResults.load(str(p), data_store=data_store)
    assert inf is None, "Should not be able to load a file that doesn't exist."

    p = tmp_path / "invalid.npy"
    with open(str(p), "w") as f:
        f.write("hello")
    inf = InferenceResults.load(str(p), data_store=data_store)
    assert inf is None, "Should not be able to load an invalid file."
