import os
import time
import json

from celery import Celery

from ar.data import export_labeled_examples
from shared.utils import save_json
from db.task import Task

from .transformers_textcat import train, evaluate_model, load_model
from .transformers_textcat import run_inference as _run_inference
from .inference_results import InferenceResults
from .utils import (
    _parse_labels, BINARY_CLASSIFICATION, MULTILABEL_CLASSIFICATION
)
from .paths import (
    _get_latest_model_version, _get_version_dir, _get_inference_dir,
    _get_inference_fname, _get_inference_density_plot_fname,
    _get_model_output_dir, _get_config_fname, _get_exported_data_fname,
    _get_data_parser_fname, _get_metrics_fname
)

app = Celery(
    # module name
    'train_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)

# NOTE:
# - Celery doesn't allow tasks to spin up other processes - I have to run it in Threads mode
# - When a model is training, even cold shutdown doesn't work

@app.task
def train_model(task_id):

    task = Task.fetch(task_id)

    version = _get_latest_model_version(task.task_id) + 1
    version_dir = _get_version_dir(task.task_id, version)
    print(f"Training model version={version} for task={task.task_id}")
    print(f"Storing results in {version_dir}")

    model_output_dir = _get_model_output_dir(version_dir)
    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)
    data_parser_fname = _get_data_parser_fname(version_dir)
    metrics_fname = _get_metrics_fname(version_dir)

    # Save config
    config = {
        'created_at': time.time(),
        'test_size': 0.3,
        'random_state': 42,
        'train_config': {
            'model_output_dir': model_output_dir,
            'num_train_epochs': 5,
        }
    }
    save_json(config_fname, config)

    # Export labeled examples
    print("Export labeled examples...")
    data = export_labeled_examples(task.task_id, outfile=data_fname)

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
    print("Train / Test split...")
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config['test_size'], random_state=config['random_state'])
    
    # Train
    print("Train model...")
    model = train(X_train, y_train, config=config['train_config'])

    # Evaluate
    print("Evaluate model...")
    print("--- Train ---")
    train_result = evaluate_model(model, X_train, y_train)
    print("--- Test ---")
    test_result = evaluate_model(model, X_test, y_test)

    save_json(metrics_fname, {
        'train': train_result,
        'test': test_result,
    })

    # Inference
    print("Run inference on all data filenames...")
    for fname in task.get_full_data_fnames():
        inf_fname = _get_inference_fname(task.task_id, version, fname)
        _run_inference(model, fname, inf_fname)

        inference_results = InferenceResults.load(inf_fname)

        import seaborn as sns
        import matplotlib.pyplot as plt

        def _plot(outname, data, title):
            print("Creating density plot:", outname)
            sns_plot = sns.distplot(data)
            plt.title(title)
            sns_plot.figure.savefig(outname)
            plt.clf()

        if problem_type == BINARY_CLASSIFICATION:
            # Plot positive class for binary classification
            class_name = class_order[0]
            outname = _get_inference_density_plot_fname(task_id, version, fname, class_name)
            _plot(outname, inference_results.get_prob_for_class(1), f'{class_name} : {fname}')
        elif problem_type == MULTILABEL_CLASSIFICATION:
            # Plot all classes for multilabel classification
            for i, class_name in enumerate(class_order):
                outname = _get_inference_density_plot_fname(task_id, version, fname, class_name)
                _plot(outname, inference_results.get_prob_for_class(i), f'{class_name} : {fname}')

    print("Done")

app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''

if __name__ == '__main__':
    # TODO fake a binary classification model
    task_id = 'cdff2935-744c-45de-a9cf-bff4a9c6264f'
    train_model(task_id)
