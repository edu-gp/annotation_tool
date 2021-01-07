# TODO remove these module dependencies if possible
import numpy as np
import pandas as pd
import torch
from simpletransformers.classification import ClassificationModel

from .utils import raw_to_pos_prob

USE_CUDA = torch.cuda.is_available()


def get_class_weights(X, y):
    from sklearn.utils.class_weight import compute_class_weight

    class_weights = compute_class_weight("balanced", np.unique(y), y)
    # Normalize class weights
    class_weights = class_weights / class_weights.max()
    print("class_weights:", class_weights)
    return class_weights


def build_model(config, model_dir=None, weight=None):
    """
    Inputs:
        config: train_config, see train_celery.py
        model_dir: a trained model's output dir, None if model has not been trained yet
        weight: class weights
    """
    print(f"Building model with config: {config}")
    return ClassificationModel(
        "roberta",
        model_dir or "roberta-base",
        use_cuda=USE_CUDA,
        args={
            # https://github.com/ThilinaRajapakse/simpletransformers/#sliding-window-for-long-sequences
            "sliding_window": config.get("sliding_window", False),
            "reprocess_input_data": True,
            "overwrite_output_dir": True,
            # Disable tokenizer cache
            "use_cached_eval_features": False,
            "no_cache": True,
            "num_train_epochs": config["num_train_epochs"],
            "weight": weight,
            # Disable checkpoints to save disk space.
            "save_eval_checkpoints": False,
            "save_model_every_epoch": False,
            "save_steps": 999999,
            # Bug in the library, need to specify it here and in the .train_model kwargs
            "output_dir": config.get("model_output_dir"),
            # Note: 512 requires 16g of GPU mem. You can try 256 for 8g.
            "max_seq_length": config.get("max_seq_length", 512),
            "train_batch_size": config.get("train_batch_size", 8),
            "eval_batch_size": config.get("eval_batch_size", 8),
        },
    )


# TODO validation data + early stopping


def train(X_train, y_train, config):
    train_df = pd.DataFrame(zip(X_train, y_train))

    weight = get_class_weights(X_train, y_train)

    model = build_model(config, weight=weight)

    # Train the model
    model.train_model(train_df, output_dir=config.get("model_output_dir"))

    return model


def evaluate_model(model, X_test, y_test):
    """
    Designed for binary classification models
    """
    if X_test is None or len(X_test) == 0:
        print("No test data given. Skipping evaluation.")
        return

    # Evaluate the model
    # test_df  = pd.DataFrame(zip(X_test, y_test))
    # _, model_outputs, _ = model.eval_model(test_df)

    _, raw = model.predict(X_test)

    from sklearn import metrics

    probs_pos_class = raw_to_pos_prob(raw)
    roc_auc = metrics.roc_auc_score(y_test, probs_pos_class)
    aupr = metrics.average_precision_score(y_test, probs_pos_class)
    preds = [int(x > 0.5) for x in probs_pos_class]
    precision, recall, fscore, support = metrics.precision_recall_fscore_support(
        y_test, preds
    )

    result = {
        "roc_auc": roc_auc,
        "aupr": aupr,
        "precision": list(precision),
        "recall": list(recall),
        "fscore": list(fscore),
    }

    print(result)
    return result
