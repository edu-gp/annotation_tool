"""
This file is designed to have minimal dependency on the rest of the codebase,
so we can use it for distributed model training.
"""

import re
import os
import json
from typing import List
from pathlib import Path

from sklearn.model_selection import train_test_split

from .paths import (
    _get_config_fname, _get_data_parser_fname,
    _get_exported_data_fname, _get_metrics_fname,
    _get_model_output_dir, _get_inference_fname,
    _get_inference_density_plot_fname
)
from .transformers_textcat import train, evaluate_model, build_model
from .inference_results import InferenceResults
from .utils import (
    BINARY_CLASSIFICATION, _parse_labels, load_original_data_text, get_env_int
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
    See: prep.py:prepare_task_for_training to see what is in version_dir.
    """

    if not force_retrain and _model_exists(version_dir):
        print("Model already exists; Skip training.")
        return

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)
    data_parser_fname = _get_data_parser_fname(version_dir)
    metrics_fname = _get_metrics_fname(version_dir)

    with open(config_fname) as f:
        config = json.loads(f.read())

    data = []
    with open(data_fname) as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))

    # TODO what if config is missing? or data is empty?

    X = [x['text'] for x in data]
    y, problem_type, class_order = _parse_labels(data)
    print(f"Detected problem type: {problem_type}")
    print(f"Detected classes: {class_order}")

    save_json(data_parser_fname, {
        'problem_type': problem_type,
        'class_order': class_order
    })

    assert problem_type == BINARY_CLASSIFICATION, 'Currently Only Supporting Binary Classification'

    # Train test split
    if config.get('test_size', 0) > 0:
        print("Train / Test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=config['test_size'],
            random_state=config.get('random_state', 42))
    else:
        print("No train test split used")
        X_train = X
        y_train = y
        X_test = None
        y_test = None

    # Train
    print("Train model...")
    if train_fn is None:
        train_fn = train

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


def inference(version_dir, fnames: List[str],
              build_model_fn=None, generate_plots=True):
    """
    Inputs:
        fnames: A list of filenames to run inference on.

    Results will be stored in version_dir/inference
    """

    # --- LOAD MODEL ---

    config = load_json(_get_config_fname(version_dir))

    # You can increase TRANSFORMER_EVAL_BATCH_SIZE for faster inference.
    config['train_config']['eval_batch_size'] = get_env_int(
        "TRANSFORMER_EVAL_BATCH_SIZE", 8)

    if build_model_fn is None:
        build_model_fn = build_model
    model = build_model_fn(config['train_config'],
                           model_dir=_get_model_output_dir(version_dir))

    # --- LOAD METADATA ---

    data_parser = load_json(_get_data_parser_fname(version_dir))
    problem_type = data_parser['problem_type']
    class_order = data_parser['class_order']

    # --- RUN INFERENCE ---

    for fname in fnames:
        # Run Inference on fname
        text = load_original_data_text(fname)
        _, raw = model.predict(text)
        inference_results = InferenceResults(raw)

        # Make sure the output dir exists and save it
        inf_output_fname = _get_inference_fname(version_dir, fname)
        inference_results.save(inf_output_fname)

        # Generate Plots
        if generate_plots:
            if problem_type == BINARY_CLASSIFICATION:
                # Plot positive class for binary classification
                class_name = class_order[0]
                class_name = re.sub('[^0-9a-zA-Z]+', '_', class_name)
                outname = _get_inference_density_plot_fname(
                    version_dir, fname, class_name)
                _plot(outname, inference_results.probs,
                      f'{class_name} : {Path(fname).name}')
            else:
                raise Exception(
                    "generate_plots only supports binary classification")
            # TODO: We don't fully support MULTILABEL_CLASSIFICATION at the moment.
            # elif problem_type == MULTILABEL_CLASSIFICATION:
            #     # Plot all classes for multilabel classification
            #     for i, class_name in enumerate(class_order):
            #         outname = _get_inference_density_plot_fname(version_dir, fname, class_name)
            #         # TODO get_prob_for_class is deprecated
            #         _plot(outname, inference_results.get_prob_for_class(i), f'{class_name} : {fname}')


def _plot(outname, data, title):
    import seaborn as sns
    import matplotlib.pyplot as plt
    print("Creating density plot:", outname)
    sns_plot = sns.distplot(data)
    plt.title(title)
    sns_plot.figure.savefig(outname)
    plt.clf()
