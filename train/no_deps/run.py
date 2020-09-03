"""
This file is designed to have minimal dependency on the rest of the codebase,
so we can use it for distributed model training.
"""

import re
import os
import json
from pathlib import Path

from .paths import (
    _get_config_fname, _get_data_parser_fname,
    _get_exported_data_fname, _get_metrics_fname,
    _get_model_output_dir, _get_inference_fname,
    _get_inference_density_plot_fname
)
from .transformers_textcat import train, evaluate_model, build_model
from .inference_results import InferenceResults
from .utils import (
    BINARY_CLASSIFICATION, load_original_data_text, get_env_int,
    _load_config, _prepare_data
)


def save_json(fname, data):
    assert fname.endswith('.json')
    with open(fname, 'w') as outfile:
        json.dump(data, outfile)


def load_json(fname):
    with open(fname) as f:
        return json.loads(f.read())


def _model_exists(version_dir):
    # Metrics is the last thing the model computes.
    # If this is exists, it means the model has finished training.
    metrics_fname = _get_metrics_fname(version_dir)
    return os.path.isfile(metrics_fname)


def train_model(version_dir, train_fn=None, force_retrain=False):
    """Train model and store model assets and evaluation metrics.
    See: prep.py:prepare_next_model_for_label to see what is in version_dir.
    """

    if not force_retrain and _model_exists(version_dir):
        print("Model already exists; Skip training.")
        return

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)
    data_parser_fname = _get_data_parser_fname(version_dir)
    metrics_fname = _get_metrics_fname(version_dir)

    problem_type, class_order, X_train, y_train, X_test, y_test = \
        _prepare_data(config_fname, data_fname)

    print(f"Detected problem type: {problem_type}")
    print(f"Detected classes: {class_order}")

    save_json(data_parser_fname, {
        'problem_type': problem_type,
        'class_order': class_order
    })

    assert problem_type == BINARY_CLASSIFICATION, 'Currently Only Supporting Binary Classification'

    # Train
    print("Train model...")
    if train_fn is None:
        train_fn = train

    config = _load_config(config_fname)
    train_config = config['train_config']
    # Depending on where we're training the model,
    # the output is relative to the version_dir.
    train_config['model_output_dir'] = _get_model_output_dir(version_dir)
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

    save_json(metrics_fname, {
        'train': train_result,
        'test': test_result,
    })


def load_model(version_dir, build_model_fn=None):
    config = load_json(_get_config_fname(version_dir))

    # You can increase TRANSFORMER_EVAL_BATCH_SIZE for faster inference.
    config['train_config']['eval_batch_size'] = get_env_int(
        "TRANSFORMER_EVAL_BATCH_SIZE", 8)

    if build_model_fn is None:
        build_model_fn = build_model
    model = build_model_fn(config['train_config'],
                           model_dir=_get_model_output_dir(version_dir))

    return model


def plot_results(version_dir, fname, inference_results) -> None:
    data_parser = load_json(_get_data_parser_fname(version_dir))

    if data_parser['problem_type'] == BINARY_CLASSIFICATION:
        # Plot positive class for binary classification
        class_name = data_parser['class_order'][0]
        class_name = re.sub('[^0-9a-zA-Z]+', '_', class_name)
        outname = _get_inference_density_plot_fname(
            version_dir, fname, class_name)
        _plot(outname, inference_results.probs,
              f'{class_name} : {Path(fname).name}')
    else:
        raise Exception("generate_plots only supports binary classification")


def inference(version_dir, fname,
              build_model_fn=None, generate_plots=True, inference_cache=None):
    """
    Inputs:
        fname: A .jsonl file to run inference on.
            Each line should contain a "text" key.

    Results will be stored in version_dir/inference
    """

    model = load_model(version_dir, build_model_fn)

    # Run Inference on fname
    text = load_original_data_text(fname)
    if inference_cache:
        # If using the cache, we first pick out the elements not in cache
        # and send those for inference.
        to_infer = []
        for t in text:
            if inference_cache.get(t) is None:
                to_infer.append(t)
        _, raw = model.predict(to_infer)
        # After we receive the results, update the cache.
        for x, y in zip(to_infer, raw):
            inference_cache[x] = y

        # Finally, using the updated cache to populate all the raw results.
        raw = []
        for t in text:
            # By now, all elements in text should have some results,
            # so we can safely use [ ] to access.
            raw.append(inference_cache[t])
    else:
        # If not using the cache, we just predict on all text.
        _, raw = model.predict(text)
        to_infer = text

    inference_results = InferenceResults(raw)

    # Make sure the output dir exists and save it
    inference_results.save(_get_inference_fname(version_dir, fname))

    # Generate Plots
    if generate_plots:
        plot_results(version_dir, fname, inference_results)

    return model, inference_results


def build_inference_cache(version_dir, fnames):
    """Build an inference cache from the previous inference results by the
    model in version_dir on the files in fnames.
    """
    lookup = {}

    for fname in fnames:
        text = load_original_data_text(fname)
        inf = InferenceResults.load(_get_inference_fname(version_dir, fname))

        if text and inf:
            for x, y in zip(text, inf.raw):
                if x:
                    lookup[x] = y

    return lookup


def _plot(outname, data, title):
    import seaborn as sns
    import matplotlib.pyplot as plt
    print("Creating density plot:", outname)
    sns_plot = sns.distplot(data)
    plt.title(title)
    sns_plot.figure.savefig(outname)
    plt.clf()
