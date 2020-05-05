import os
import json


def fake_train_model(model, filestore_base_dir):
    metrics_fname = os.path.join(filestore_base_dir, 'models', model.uuid,
                                 str(model.version), 'metrics.json')
    os.makedirs(os.path.dirname(metrics_fname), exist_ok=True)
    with open(metrics_fname, 'w') as f:
        f.write(json.dumps({'accuracy': 0.95}))
