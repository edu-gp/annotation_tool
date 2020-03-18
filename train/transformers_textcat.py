import pandas as pd
import numpy as np
import json
from simpletransformers.classification import ClassificationModel

from .inference_results import InferenceResults
from .utils import load_original_data_text, raw_to_pos_prob

import torch
USE_CUDA = torch.cuda.is_available()

# TODO multilabel classification

def get_class_weights(X, y):
    # TODO is this the best way?
    from sklearn.utils.class_weight import compute_class_weight
    class_weights = compute_class_weight('balanced', np.unique(y), y)
    print('class_weights:', class_weights)
    return class_weights

# TODO validation data + early stopping
def train(X_train, y_train, config):
    train_df = pd.DataFrame(zip(X_train, y_train))

    model = ClassificationModel(
            'roberta', 'roberta-base',
            use_cuda=USE_CUDA,
            args={
                # https://github.com/ThilinaRajapakse/simpletransformers/#sliding-window-for-long-sequences
                'sliding_window': config.get('sliding_window', False),

                'reprocess_input_data': True,
                'overwrite_output_dir': True,

                'use_cached_eval_features': False,
                'no_cache': True,

                'num_train_epochs': config['num_train_epochs'],
                'weight': get_class_weights(X_train, y_train),

                # TODO I don't need checkpoints yet - disable this to save disk space
                'save_eval_checkpoints': False,
                'save_model_every_epoch': False,
                'save_steps': 999999,

                # Bug in the library, need to specify it here and in the .train_model kwargs
                'output_dir': config.get('model_output_dir'),

                # Note: 512 requires 16g of GPU mem. You can try 256 for 8g.
                'max_seq_length': config.get('max_seq_length', 512),
            }
    )
    # You can set class weights by using the optional weight argument

    # Train the model
    model.train_model(
        train_df,
        output_dir=config.get('model_output_dir'),
    )

    return model

# TODO take in model as a string (directory of output)
def evaluate_model(model, X_test, y_test):
    '''
    Designed for binary classification models
    '''
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

    preds = [int(x > 0.5) for x in probs_pos_class]
    precision, recall, fscore, support = metrics.precision_recall_fscore_support(y_test, preds)

    result = {
        'roc_auc': roc_auc,
        'precision': list(precision),
        'recall': list(recall),
        'fscore': list(fscore),
    }
    
    print(result)
    return result


def load_model(model_output_dir):
    return ClassificationModel(
        'roberta', model_output_dir,
        use_cuda=USE_CUDA
    )

def run_inference(model, input_fname, output_fname):
    '''
    Input is a jsonl, like the original data we want to label
    '''
    text = load_original_data_text(input_fname)
    preds, raw = model.predict(text)
    InferenceResults(raw).save(output_fname)
    return InferenceResults
