import os
import time
import json

from celery import Celery

from ar.data import export_labeled_examples
from shared.utils import save_json, load_json, load_jsonl
from db.task import Task

from .transformers_textcat import train, evaluate_model, build_model
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
        # TODO: Rename "train_config" to "model_config", or something more generic.
        # since train_config also includes config for inference...
        # NOTE: Env vars are used as global defaults. Eventually let user pass in
        # custom configs.
        'train_config': {
            'model_output_dir': model_output_dir,
            'num_train_epochs': os.environ.get("TRANSFORMER_TRAIN_EPOCHS", 5),
            'sliding_window': os.environ.get("TRANSFORMER_SLIDING_WINDOW", True),
            'max_seq_length': os.environ.get("TRANSFORMER_MAX_SEQ_LENGTH", 512),
            'train_batch_size': os.environ.get("TRANSFORMER_TRAIN_BATCH_SIZE", 8),
            # NOTE: Specifying a large batch size during inference makes the
            # process take up unnessesarily large amounts of memory.
            # We'll only toggle this on at inference time.
            # 'eval_batch_size': os.environ.get("TRANSFORMER_EVAL_BATCH_SIZE", 8),
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

    # Note: It appears inference can be faster if it's allowed to use all the GPU memory,
    # however the only way to clear all GPU memory is to end this task. So we call inference
    # asynchronously so this task can end.
    post_training_inference.delay(task_id, version)


@app.task
def post_training_inference(task_id, version):
    """
    Inputs:
        task_id: -
        version: The model version
    
    Note: This only works for binary classification models
    """
    task = Task.fetch(task_id)

    # version = _get_latest_model_version(task.task_id) + 1
    version_dir = _get_version_dir(task.task_id, version)
    print(f"Post training inference model version={version} for task={task.task_id}")

    model_output_dir = _get_model_output_dir(version_dir)
    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)

    # Save config
    config = load_json(config_fname)

    # You can set TRANSFORMER_EVAL_BATCH_SIZE to a larger number for faster inference.
    config['train_config']['eval_batch_size'] = os.environ.get("TRANSFORMER_EVAL_BATCH_SIZE", 8)

    # Load exported labeled examples
    data = load_jsonl(data_fname, to_df=False)

    _, problem_type, class_order = _parse_labels(data)
  
    model = build_model(config['train_config'], model_dir=model_output_dir)

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
            _plot(outname, inference_results.probs, f'{class_name} : {fname}')
        else:
            raise Exception("post_training_inference only supports binary classification")
        # TODO: We don't fully support MULTILABEL_CLASSIFICATION at the moment.
        # elif problem_type == MULTILABEL_CLASSIFICATION:
        #     # Plot all classes for multilabel classification
        #     for i, class_name in enumerate(class_order):
        #         outname = _get_inference_density_plot_fname(task_id, version, fname, class_name)
        #         # TODO get_prob_for_class is deprecated
        #         _plot(outname, inference_results.get_prob_for_class(i), f'{class_name} : {fname}')


app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''