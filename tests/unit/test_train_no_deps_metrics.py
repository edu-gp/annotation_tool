import pandas as pd
from pandas.testing import assert_series_equal

from train.no_deps.metrics import compute_metrics

from shared.utils import save_jsonl, save_json
from train.no_deps.paths import (
    _get_config_fname, _get_exported_data_fname
)


def _setup(tmpdir):
    # Note: See test_train_no_deps_run:test__compute_metrics to see how
    # train & test sets are split.

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


def test__compute_metrics(tmpdir):
    _setup(tmpdir)

    df = pd.DataFrame([
        {"text": "yes1", "probs": 0.9},
        {"text": "yes2", "probs": 0.8},
        {"text": "yes3", "probs": 0.7},
        {"text": "yes4", "probs": 0.6},
        {"text": "no1", "probs": 0.1},
        {"text": "no2", "probs": 0.2},
        {"text": "no3", "probs": 0.3},
        {"text": "no4", "probs": 0.4},
    ])

    metrics = compute_metrics(tmpdir, df, threshold=0.5)

    assert metrics == {
        'train': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [3, 2],
            'ro': 1.0,
            'tn': 3,
            'fp': 0,
            'fn': 0,
            'tp': 2
        },
        'test': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [1, 2],
            'ro': 1.0,
            'tn': 1,
            'fp': 0,
            'fn': 0,
            'tp': 2
        },
        'not_found': []
    }


def test__compute_metrics__with_threshold(tmpdir):
    _setup(tmpdir)

    df = pd.DataFrame([
        {"text": "yes1", "probs": 0.9},
        {"text": "yes2", "probs": 0.8},
        {"text": "yes3", "probs": 0.7},
        {"text": "yes4", "probs": 0.6},
        {"text": "no1", "probs": 0.1},
        {"text": "no2", "probs": 0.2},
        {"text": "no3", "probs": 0.3},
        {"text": "no4", "probs": 0.4},
    ])

    metrics = compute_metrics(tmpdir, df, threshold=0.85)

    # Use `assert_series_equal` for float equality barring numerical errors.
    assert_series_equal(
        pd.Series(metrics),
        pd.Series({
            'train': {
                'pr': [0.6, 0.0],
                're': [1.0, 0.0],
                'f1': [0.75, 0.0],
                'su': [3, 2],
                'ro': 1.0,
                'tn': 3,
                'fp': 0,
                'fn': 2,
                'tp': 0
            },
            'test': {
                'pr': [0.5, 1.0],
                're': [1.0, 0.5],
                'f1': [0.6666666666666666, 0.6666666666666666],
                'su': [1, 2],
                'ro': 1.0,
                'tn': 1,
                'fp': 0,
                'fn': 1,
                'tp': 1
            },
            'not_found': []
        }), check_exact=False)


def test__compute_metrics__invalid_lookup(tmpdir):
    _setup(tmpdir)

    df = pd.DataFrame([
        # Missing lookup for "yes1" and "no1"
        #{"text": "yes1", "probs": 0.9},
        {"text": "yes2", "probs": 0.8},
        {"text": "yes3", "probs": 0.7},
        {"text": "yes4", "probs": 0.6},
        # {"text": "no1", "probs": 0.1},
        {"text": "no2", "probs": 0.2},
        {"text": "no3", "probs": 0.3},
        {"text": "no4", "probs": 0.4},
    ])

    metrics = compute_metrics(tmpdir, df, threshold=0.5)

    assert metrics == {
        'train': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [2, 2],
            'ro': 1.0,
            'tn': 2,
            'fp': 0,
            'fn': 0,
            'tp': 2
        },
        'test': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [1, 1],
            'ro': 1.0,
            'tn': 1,
            'fp': 0,
            'fn': 0,
            'tp': 1
        },
        "not_found": ["no1", "yes1"]
    }


def test__compute_metrics__invalid_lookup_2(tmpdir):
    _setup(tmpdir)

    df = pd.DataFrame([
        # Duplicate lookup for "yes1" and "no1"
        # Current behaviour is just to pick one of them.
        {"text": "yes1", "probs": 0.9},
        {"text": "yes1", "probs": 0.91},
        {"text": "yes2", "probs": 0.8},
        {"text": "yes3", "probs": 0.7},
        {"text": "yes4", "probs": 0.6},
        {"text": "no1", "probs": 0.1},
        {"text": "no1", "probs": 0.11},
        {"text": "no2", "probs": 0.2},
        {"text": "no3", "probs": 0.3},
        {"text": "no4", "probs": 0.4},
    ])

    metrics = compute_metrics(tmpdir, df, threshold=0.5)

    assert metrics == {
        'train': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [3, 2],
            'ro': 1.0,
            'tn': 3,
            'fp': 0,
            'fn': 0,
            'tp': 2
        },
        'test': {
            'pr': [1.0, 1.0],
            're': [1.0, 1.0],
            'f1': [1.0, 1.0],
            'su': [1, 2],
            'ro': 1.0,
            'tn': 1,
            'fp': 0,
            'fn': 0,
            'tp': 2
        },
        'not_found': []
    }


def test__compute_metrics__sensible_defaults(tmpdir):
    _setup(tmpdir)

    # We cannot find inference for any data points, in this case the metrics
    # should fall back to sensible defaults.
    df = pd.DataFrame([
        {"text": "blah", "probs": 0.5},
    ])

    metrics = compute_metrics(tmpdir, df, threshold=0.5)

    # Use assert_series_equal to compare nan's
    assert_series_equal(
        pd.Series(metrics),
        pd.Series({
            'train': {
                'pr': [float('nan'), float('nan')],
                're': [float('nan'), float('nan')],
                'f1': [float('nan'), float('nan')],
                'su': [float('nan'), float('nan')],
                'ro': float('nan'),
                'tn': float('nan'),
                'fp': float('nan'),
                'fn': float('nan'),
                'tp': float('nan')
            },
            'test': {
                'pr': [float('nan'), float('nan')],
                're': [float('nan'), float('nan')],
                'f1': [float('nan'), float('nan')],
                'su': [float('nan'), float('nan')],
                'ro': float('nan'),
                'tn': float('nan'),
                'fp': float('nan'),
                'fn': float('nan'),
                'tp': float('nan')
            },
            'not_found': ['no4', 'yes3', 'no1', 'yes4', 'no3', 'yes2', 'no2', 'yes1']
        }), check_exact=False)
