import pandas as pd
from sklearn.metrics import (
    precision_recall_fscore_support, roc_auc_score, confusion_matrix
)
from .paths import _get_config_fname, _get_exported_data_fname
from .run import _prepare_data


def compute_metrics(version_dir, inference_lookup_df, threshold: float = 0.5):
    """
    Inputs:
        version_dir: Directory of the model.
        inference_lookup_df: A pandas dataframe with 2 cols ['text', 'probs']
            containing the inference results from a model.
        threshold: The cutoff threshold for this binary classifier \in [0,1]

    Returns:
    {
        "train": {
            "pr": [neg_class:float, pos_class:float],
            "re": [neg_class:float, pos_class:float],
            "f1": [neg_class:float, pos_class:float],
            "su": [neg_class:int, pos_class:int],
            "ro": float,
            "tn": int (count),
            "tp": int (count),
            "fn": int (count),
            "fp": int (count),
        },
        "test": {
            # same stats as train
        }
        "not_found": List[str] of examples we couldn't get inference for
    }

    Where the keys stand for:
    - pr: Precision
    - re: Recall
    - f1: F1 Score
    - su: Support, the number of items considered for this metric
    - ro: ROC AUC
    - tn: True Negative
    - tp: True Positive
    - fn: False Negative
    - fp: False Positive

    Will return an object with sensible defaults if the metrics cannot be
    computed (e.g. if model has not been trained).
    """

    # TODO: Refactoring model.export_inference to only depend on version_dir,
    # then compute `inference_lookup_df` in this function.

    # TODO cache this function or parts of it (especially inference_lookup_df).
    # Unless we change the UI, performance will be an issue.

    # Make sure inference_lookup_df is valid and there are no duplicate rows.
    assert 'text' in inference_lookup_df.columns
    assert 'probs' in inference_lookup_df.columns
    inference_lookup_df = inference_lookup_df[['text', 'probs']] \
        .drop_duplicates(subset=['text'], keep='first')

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)

    _, _, X_train, y_train, X_test, y_test = \
        _prepare_data(config_fname, data_fname)

    not_found = []

    def _get_pred_result(X, y, df):
        nonlocal not_found

        res = pd.DataFrame(zip(X, y), columns=['text', 'y'])
        res = res.merge(df, on='text', how='left')

        not_found += list(res[res['probs'].isna()]['text'])

        res = res.dropna(subset=['probs'])

        return res

    def _get_stats(res, threshold):
        res['preds'] = (res['probs'] > threshold).astype(int)

        pr, re, f1, su = \
            precision_recall_fscore_support(res['y'], res['preds'])
        pr = list(pr)
        re = list(re)
        f1 = list(f1)
        su = list(su)
        if not pr:
            pr = [float('nan'), float('nan')]
        if not re:
            re = [float('nan'), float('nan')]
        if not f1:
            f1 = [float('nan'), float('nan')]
        if not su:
            su = [float('nan'), float('nan')]

        try:
            ro = roc_auc_score(res['y'], res['probs'])
        except ValueError:
            ro = float('nan')

        try:
            tn, fp, fn, tp = confusion_matrix(res['y'], res['preds']).ravel()
        except ValueError:
            tn = fp = fn = tp = float('nan')

        return {
            'pr': pr,
            're': re,
            'f1': f1,
            'su': su,
            'ro': ro,
            'tn': tn,
            'fp': fp,
            'fn': fn,
            'tp': tp,
        }

    stats = {
        'train': _get_stats(_get_pred_result(X_train, y_train, inference_lookup_df), threshold),
        'test': _get_stats(_get_pred_result(X_test, y_test, inference_lookup_df), threshold),
        'not_found': not_found
    }

    return stats
