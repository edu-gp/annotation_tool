import pandas as pd
from sklearn.metrics import (
    confusion_matrix,
    precision_recall_fscore_support,
    average_precision_score,
)

from .paths import _get_config_fname, _get_exported_data_fname
from .utils import _prepare_data


class InferenceMetrics:
    def __init__(self, df):
        """
        Inputs:
            df: A dataframe with at least columns ['text', 'probs'],
                representing the inference outputs from a model.
        """
        assert "text" in df.columns
        assert "probs" in df.columns
        df = df[["text", "probs"]].drop_duplicates(subset=["text"], keep="first")
        self.df = df

    def compute_metrics(self, X, y, threshold):
        """
        Inputs:
            X: A list of text
            y: A list of ground truth binary labels \in {0,1}
            threshold: The cutoff threshold for this binary classifier \in [0,1]
        Returns:
            stats: A dictionary of stats
            not_found: A list of strings that were in `X` but not in `self.df`.
        """
        not_found = []

        res = pd.DataFrame(zip(X, y), columns=["text", "y"])
        print(res)
        res = res.merge(self.df, on="text", how="left")
        print(res)

        not_found += list(res[res["probs"].isna()]["text"])
        print(not_found)

        res = res.dropna(subset=["probs"])

        res["preds"] = (res["probs"] > threshold).astype(int)

        pr, re, f1, su = precision_recall_fscore_support(res["y"], res["preds"])
        pr = list(pr)
        re = list(re)
        f1 = list(f1)
        su = list(su)
        if not pr:
            pr = [float("nan"), float("nan")]
        if not re:
            re = [float("nan"), float("nan")]
        if not f1:
            f1 = [float("nan"), float("nan")]
        if not su:
            su = [float("nan"), float("nan")]

        try:
            ro = average_precision_score(res["y"], res["probs"])
        except ValueError and IndexError:
            ro = float("nan")

        try:
            tn, fp, fn, tp = confusion_matrix(res["y"], res["preds"]).ravel()
        except ValueError:
            tn = fp = fn = tp = float("nan")

        stats = {
            "pr": pr,
            "re": re,
            "f1": f1,
            "su": su,
            "ro": ro,
            "tn": tn,
            "fp": fp,
            "fn": fn,
            "tp": tp,
        }

        return stats, not_found


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

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)

    _, _, X_train, y_train, X_test, y_test = _prepare_data(config_fname, data_fname)

    im = InferenceMetrics(inference_lookup_df)

    tr_stats, tr_not_found = im.compute_metrics(X_train, y_train, threshold)
    ts_stats, ts_not_found = im.compute_metrics(X_test, y_test, threshold)

    stats = {
        "train": tr_stats,
        "test": ts_stats,
        "not_found": tr_not_found + ts_not_found,
    }

    return stats
