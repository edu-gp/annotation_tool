import json
from typing import List
from db.task import Task
from ar.data import (
    save_new_ar_for_user,
    annotate_ar,
    fetch_all_annotations
)
from ar.utils import get_ar_id
from train.prep import (
    get_next_version,
    prepare_task_for_training
)
from train.no_deps.run import (
    train_model, inference
)
from train.no_deps.paths import (
    # Prep Model
    _get_config_fname,
    _get_exported_data_fname,
    # Train Model
    _get_data_parser_fname,
    _get_metrics_fname,
    # Model Inference
    _get_inference_fname,
)
from train.no_deps.utils import BINARY_CLASSIFICATION
from train.no_deps.inference_results import InferenceResults
from shared.utils import load_json, load_jsonl
import numpy as np

# TODO:
# 1. Test train data too few (an error would be thrown but we should cover it)
# 2. Test train data all one class
# 3. Test not binary classification (should throw NotSupported)


class stub_model:
    def predict(self, text: List[str]):
        preds = np.array([1] * len(text))
        # This is the style when "Sliding Window" is enabled
        probs = [np.array([[-0.48803285,  0.56392884]],
                          dtype=np.float32)] * len(text)
        return preds, probs


def stub_train_fn(X, y, config):
    return stub_model()


def stub_build_fn(config, model_dir):
    return stub_model()


class TestTrainFlow:
    def setup_method(self, test_method):
        task = Task('just testing ar')
        task.task_id = '__testing_'+task.task_id
        task.save()

        # Create 10 negative examples and 10 positive examples
        task_id = task.task_id
        user_id = 'eddie_test'
        for line in range(20):
            if line < 10:
                ar = {
                    'ar_id': get_ar_id('a', line), 'fname': 'a', 'line_number': line, 'score': 0.9,
                    'data': {'text': 'banker', 'meta': {'foo': 'bar'}}
                }
                my_anno = {'labels': {'HEALTHCARE': -1}}
            else:
                ar = {
                    'ar_id': get_ar_id('a', line), 'fname': 'a', 'line_number': line, 'score': 0.9,
                    'data': {'text': 'doctor', 'meta': {'foo': 'bar'}}
                }
                my_anno = {'labels': {'HEALTHCARE': 1}}

            save_new_ar_for_user(task_id, user_id, [ar])
            annotate_ar(task_id, user_id, ar['ar_id'], my_anno)

        self.task = task

    def teardown_method(self, test_method):
        # Make sure we delete the task even when the test fails.
        self.task.delete()

    def test__test_setup_is_ok(self):
        task = self.task
        assert len(fetch_all_annotations(task.task_id, 'eddie_test')) == 20

    def test__train_flow(self, tmpdir):
        task = self.task
        version = get_next_version(task.task_id)
        assert version == 1

        # Part 1. Prepare data for training.
        version_dir = prepare_task_for_training(task.task_id, version)

        config = load_json(_get_config_fname(version_dir))
        assert config is not None
        assert config['train_config'] is not None

        data = load_jsonl(_get_exported_data_fname(version_dir), to_df=False)
        assert data[0] == {'labels': {'HEALTHCARE': -1}, 'text': 'banker'}

        # Part 2. Train model.
        train_model(version_dir, train_fn=stub_train_fn)

        data_parser_results = load_json(_get_data_parser_fname(version_dir))
        assert data_parser_results['problem_type'] == BINARY_CLASSIFICATION

        metrics = load_json(_get_metrics_fname(version_dir))
        assert metrics['test'] is not None
        assert metrics['train'] is not None

        # Part 3. Post-training Inference.
        fnames = []

        content = [
            {'text': 'hello'},
            {'text': 'world'}
        ]
        f = tmpdir.mkdir("sub").join("hello.jsonl")
        f.write('\n'.join([json.dumps(x) for x in content]))
        fnames.append(str(f))

        inference(version_dir, fnames,
                  build_model_fn=stub_build_fn, generate_plots=False)

        ir = InferenceResults.load(
            _get_inference_fname(version_dir, fnames[0]))
        assert np.isclose(ir.probs, [0.7411514, 0.7411514]).all()
