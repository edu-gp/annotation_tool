import os
from pathlib import Path

from .utils import listdir

"""
A model's directory consists of:
- config.json : Model training and inference configurations.
- data.jsonl : The original data from which the model was trained and the
    evaluation metrics were calculated.
- data_parser.json : (Not used often) A generated file that stores metadata
    derived from `data.jsonl`, such as is the task binary or multi-class
    classification.
- metrics.json : A generated file storing the training and test metrics of the
    model, created after a model has been successfully trained. Note we usually
    don't use this file for metrics anymore, we comupte it on the fly using
    db.model.Model.compute_metrics instead.
- model/ : A directory of the actual model assets, e.g. weights, vocab, etc.
- inference/ : Past inferences. Under this directory, you'll find:
    - <dataset>.pred.npy : The raw inference file for `dataset`.
        Use train.no_deps.inference_results.InferenceResults.load to view the
        results.
    - <dataset>.pred.<label_name>.histogram: A png of the histogram of positive
        class probabilities for `label_name`.
"""


def _get_model_output_dir(version_dir):
    return os.path.join(version_dir, "model")


def _get_config_fname(version_dir):
    return os.path.join(version_dir, "config.json")


def _get_exported_data_fname(version_dir):
    return os.path.join(version_dir, "data.jsonl")


def _get_data_parser_fname(version_dir):
    return os.path.join(version_dir, "data_parser.json")


def _get_metrics_fname(version_dir):
    return os.path.join(version_dir, "metrics.json")


def _get_metrics_v2_fname(version_dir, threshold):
    threshold = round(threshold, 8)
    return os.path.join(version_dir, f"metrics_v2_threshold={threshold}.p")


def _get_inference_dir(version_dir):
    # Note: We're responsible for creating this dir if it doesn't exist yet.
    dirname = os.path.join(version_dir, "inference")
    os.makedirs(dirname, exist_ok=True)
    return dirname


def _get_inference_fname(version_dir, data_fname):
    stem = Path(data_fname).stem
    return os.path.join(_get_inference_dir(version_dir), f"{stem}.pred")


def _get_inference_density_plot_fname(version_dir, data_fname, class_name):
    stem = Path(data_fname).stem
    return os.path.join(
        _get_inference_dir(version_dir), f"{stem}.pred.{class_name}.histogram.png"
    )


def _get_all_plots(version_dir, data_store):
    dirname = _get_inference_dir(version_dir)
    res = []
    if data_store == 'local' and not os.path.isdir(dirname):
        return []

    for f in listdir(dirname, data_store=data_store):
        if f.endswith(".histogram.png"):
            res.append(f"{dirname}/{f}")
    return res


def _get_all_inference_fnames(version_dir, data_store):
    dirname = _get_inference_dir(version_dir)
    res = []
    if data_store == 'local' and not os.path.isdir(dirname):
        return []

    for f in listdir(dirname, data_store=data_store):
        if f.endswith(".pred.npy"):
            res.append(f"{dirname}/{f}")
    return res


def _inference_fnames_to_datasets(fnames):
    # Only retain files with the valid ending, and convert them to .jsonl
    ending = ".pred.npy"

    res = []
    for fname in fnames:
        # Remove any directory prefix.
        fname = Path(fname).name
        if fname.endswith(ending):
            # Swap ending with '.jsonl'
            fname = fname[: -len(ending)] + ".jsonl"
            res.append(fname)

    return res
