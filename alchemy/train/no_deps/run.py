"""
This file is designed to have minimal dependency on the rest of the codebase,
so we can use it for distributed model training.
"""

import re
from pathlib import Path

from envparse import env

from .inference_results import InferenceResults
from .paths import (
    _get_all_inference_fnames,
    _get_config_fname,
    _get_data_parser_fname,
    _get_exported_data_fname,
    _get_inference_density_plot_fname,
    _get_inference_fname,
    _get_metrics_fname,
    _get_model_output_dir,
    _inference_fnames_to_datasets,
)
from .storage_manager import DatasetStorageManager
from .transformers_textcat import build_model, evaluate_model, train
from .utils import (
    BINARY_CLASSIFICATION, load_original_data_text,
    _prepare_data, load_json, save_json, file_exists
)


def _model_exists(version_dir, data_store):
    # Metrics is the last thing the model computes.
    # If this is exists, it means the model has finished training.
    metrics_fname = _get_metrics_fname(version_dir)
    return file_exists(metrics_fname, data_store)


def train_model(version_dir, data_store, train_fn=None, force_retrain=False):
    """Train model and store model assets and evaluation metrics.
    See: prep.py:prepare_next_model_for_label to see what is in version_dir.
    """

    if not force_retrain and _model_exists(version_dir, data_store=data_store):
        print("Model already exists; Skip training.")
        return

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)
    data_parser_fname = _get_data_parser_fname(version_dir)
    metrics_fname = _get_metrics_fname(version_dir)

    problem_type, class_order, X_train, y_train, X_test, y_test = _prepare_data(
        config_fname, data_fname, data_store=data_store
    )

    print(f"Detected problem type: {problem_type}")
    print(f"Detected classes: {class_order}")

    save_json(
        data_parser_fname, {"problem_type": problem_type, "class_order": class_order}, data_store=data_store
    )

    assert (
        problem_type == BINARY_CLASSIFICATION
    ), "Currently Only Supporting Binary Classification"

    # Train
    print("Train model...")
    if train_fn is None:
        train_fn = train

    config = load_json(config_fname, data_store=data_store)
    train_config = config["train_config"]
    # Depending on where we're training the model,
    # the output is relative to the version_dir.
    train_config["model_output_dir"] = _get_model_output_dir(version_dir)
    model = train_fn(X_train, y_train, config=train_config)

    # Evaluate
    print("Evaluate model...")
    print("--- Train ---")
    train_result = evaluate_model(model, X_train, y_train)

    if X_test is not None:
        print("--- Test ---")
        test_result = evaluate_model(model, X_test, y_test)
    else:
        test_result = {}

    save_json(metrics_fname, {"train": train_result, "test": test_result}, data_store=data_store)


def load_model(version_dir, data_store, build_model_fn=None):
    config = load_json(_get_config_fname(version_dir), data_store=data_store)

    # You can increase TRANSFORMER_EVAL_BATCH_SIZE for faster inference.
    config['train_config']['eval_batch_size'] = env.int(
        "TRANSFORMER_EVAL_BATCH_SIZE", default=8)

    if build_model_fn is None:
        build_model_fn = build_model
    model = build_model_fn(
        config["train_config"], model_dir=_get_model_output_dir(version_dir)
    )

    return model


def plot_results(version_dir, fname, inference_results, data_store) -> None:
    data_parser = load_json(_get_data_parser_fname(version_dir), data_store=data_store)

    if data_parser["problem_type"] == BINARY_CLASSIFICATION:
        # Plot positive class for binary classification
        class_name = data_parser["class_order"][0]
        class_name = re.sub("[^0-9a-zA-Z]+", "_", class_name)
        outname = _get_inference_density_plot_fname(version_dir, fname, class_name)
        _plot(outname, inference_results.probs, f"{class_name} : {Path(fname).name}", data_store=data_store)
    else:
        raise Exception("generate_plots only supports binary classification")


def _plot(outname, data, title):
    import seaborn as sns
    import matplotlib.pyplot as plt

    print("Creating density plot:", outname)
    sns_plot = sns.distplot(data)
    plt.title(title)
    sns_plot.figure.savefig(outname)
    plt.clf()


class InferenceCache:
    def __init__(self):
        self.lookup_ = {}

    def set(self, text, value):
        self.lookup_[self.hash(text)] = value

    def get(self, text):
        return self.lookup_.get(self.hash(text))

    def hash(self, text):
        # TODO choose some hash function
        return text


def build_inference_cache(
    version_dir: str, dsm: DatasetStorageManager, data_store
) -> InferenceCache:
    """Build an inference cache from the previous inference results by the
    model in version_dir on the files in fnames.
    """
    cache = InferenceCache()

    prev_inf_fnames = _get_all_inference_fnames(version_dir, data_store=data_store)
    prev_inf_datasets = _inference_fnames_to_datasets(prev_inf_fnames)

    for dataset in prev_inf_datasets:
        dataset_path = dsm.download(dataset)
        text = load_original_data_text(dataset_path, data_store=data_store)
        inf = InferenceResults.load(_get_inference_fname(version_dir, dataset_path), data_store=data_store)

        if text and inf:
            for x, y in zip(text, inf.raw):
                if x:  # Ignore cases when the text is empty.
                    cache.set(x, y)

    return cache


def inference(
    version_dir,
    dataset_local_path,
    data_store,
    build_model_fn=None,
    generate_plots=True,
    inference_cache: InferenceCache = None,
):
    """
    Inputs:
        dataset_local_path: A .jsonl file to run inference on.
            Each line should contain a "text" key.

    Results will be stored in version_dir/inference
    """

    model = load_model(version_dir, build_model_fn=build_model_fn, data_store=data_store)

    # Run Inference on dataset
    text = load_original_data_text(dataset_local_path, data_store=data_store)
    if inference_cache:
        # If using the cache, we first pick out the elements not in cache
        # and send those for inference in batch.
        to_infer = []
        for t in text:
            if inference_cache.get(t) is None:
                to_infer.append(t)
        _, raw = model.predict(to_infer)
        # After we receive the results, update the cache.
        for x, y in zip(to_infer, raw):
            inference_cache.set(x, y)

        # Finally, using the updated cache to populate all the raw results.
        raw = []
        for t in text:
            # By now, all elements in text should have some results,
            # so we can safely use [ ] to access.
            raw.append(inference_cache.get(t))
    else:
        # If not using the cache, we just predict on all text.
        _, raw = model.predict(text)

    inference_results = InferenceResults(raw, data_store=data_store)

    # Make sure the output dir exists and save it
    inference_results.save(_get_inference_fname(version_dir, dataset_local_path))

    # Generate Plots
    if generate_plots:
        plot_results(version_dir, dataset_local_path, inference_results, data_store=data_store)

    return model, inference_results
