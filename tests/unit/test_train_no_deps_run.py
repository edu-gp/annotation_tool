from alchemy.train.no_deps.utils import _prepare_data

from alchemy.shared.utils import save_jsonl, save_json
from alchemy.train.no_deps.utils import BINARY_CLASSIFICATION
from alchemy.train.no_deps.paths import (
    _get_config_fname, _get_exported_data_fname
)


def test__prepare_data(tmpdir):
    config_fname = _get_config_fname(tmpdir)
    data_fname = _get_exported_data_fname(tmpdir)

    save_json(config_fname, {
        "created_at": 1594824129.6124663,
        "test_size": 0.3,
        "random_state": 42,
        "train_config": {
            "num_train_epochs": 5,
            "sliding_window": False,
            "max_seq_length": 512,
            "train_batch_size": 8
        }
    })
    save_jsonl(data_fname, [
        {"text": "yes1", "labels": {"foo": 1}},
        {"text": "yes2", "labels": {"foo": 1}},
        {"text": "yes3", "labels": {"foo": 1}},
        {"text": "yes4", "labels": {"foo": 1}},
        {"text": "no1", "labels": {"foo": -1}},
        {"text": "no2", "labels": {"foo": -1}},
        {"text": "no3", "labels": {"foo": -1}},
        {"text": "no4", "labels": {"foo": -1}},
    ])

    problem_type, class_order, X_train, y_train, X_test, y_test = \
        _prepare_data(config_fname, data_fname)

    assert problem_type == BINARY_CLASSIFICATION
    assert class_order == ['foo']
    assert X_train == ['no4', 'yes3', 'no1', 'yes4', 'no3']
    assert y_train == [0, 1, 0, 1, 0]
    assert X_test == ['yes2', 'no2', 'yes1']
    assert y_test == [1, 0, 1]


def test__prepare_data_with_invalid_data(tmpdir):
    """By current design, _prepare_data is not responsible for checking the
    validity of the data; that should've been taken care of earlier.
    """
    config_fname = _get_config_fname(tmpdir)
    data_fname = _get_exported_data_fname(tmpdir)

    save_json(config_fname, {
        "created_at": 1594824129.6124663,
        "test_size": 0.3,
        "random_state": 42,
        "train_config": {
            "num_train_epochs": 5,
            "sliding_window": False,
            "max_seq_length": 512,
            "train_batch_size": 8
        }
    })
    save_jsonl(data_fname, [
        {"text": "", "labels": {"foo": 1}},
        {"text": "", "labels": {"foo": 1}},
        {"text": "", "labels": {"foo": 1}},
        {"text": "", "labels": {"foo": 1}},
        {"text": "", "labels": {"foo": -1}},
        {"text": "", "labels": {"foo": -1}},
        {"text": "", "labels": {"foo": -1}},
        {"text": "", "labels": {"foo": -1}},
    ])

    problem_type, class_order, X_train, y_train, X_test, y_test = \
        _prepare_data(config_fname, data_fname)

    assert problem_type == BINARY_CLASSIFICATION
    assert class_order == ['foo']
    assert X_train == ['', '', '', '', '']
    assert y_train == [0, 1, 0, 1, 0]
    assert X_test == ['', '', '']
    assert y_test == [1, 0, 1]
