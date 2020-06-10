import os
import tempfile
from pathlib import Path
from .run import (
    train_model as _train_model,
    inference as _inference,
    build_inference_cache
)
from .utils import (
    gs_copy_dir as copy_dir,
    gs_copy_file as copy_file
)
from .paths import (
    _get_all_inference_fnames,
    _inference_fnames_to_original_fnames
)


def train_model(local_dir, remote_dir, force_retrain=False):
    # Download config and data
    copy_dir(remote_dir, local_dir)

    # Train Model
    _train_model(local_dir, force_retrain=force_retrain)

    # Upload trained model
    copy_dir(local_dir, remote_dir)


def download_files_to_local(fnames, local_data_dir):
    local_fnames = []
    for fname in fnames:
        local_fname = os.path.join(local_data_dir, Path(fname).name)
        copy_file(fname, local_fname)
        local_fnames.append(local_fname)
    return local_fnames


def inference(local_dir, remote_dir, local_fname, inference_cache=None):
    # Download a trained model
    copy_dir(remote_dir, local_dir)

    # Run Inference - results saved in local_dir
    _inference(local_dir, local_fname, inference_cache)

    # Upload results
    copy_dir(local_dir, remote_dir)


def run(remote_dirs, infer_fnames, force_retrain, eval_batch_size):
    """
    For all models in `remote_dirs`
        Train if `force_retrain`
        For all files in `infer_fnames`
            Run model inference on those files using bsize `eval_batch_size`
    """
    print("Running Training & Inference")
    print(f"remote_dirs={remote_dirs}")
    print(f"infer_fnames={infer_fnames}")
    print(f"force_retrain={force_retrain}")
    print(f"eval_batch_size={eval_batch_size}")

    if isinstance(eval_batch_size, int):
        os.environ['TRANSFORMER_EVAL_BATCH_SIZE'] = str(eval_batch_size)

    if remote_dir is not None:
        print(f"Executing Training Script")

        with tempfile.TemporaryDirectory() as local_dir:
            local_model_dir = os.path.join(local_dir, 'model')
            local_data_dir = os.path.join(local_dir, 'data')

            os.makedirs(local_model_dir, exist_ok=True)
            os.makedirs(local_data_dir, exist_ok=True)

            # This will train the model if `force_retrain` or it doesn't exist.
            train_model(local_model_dir, remote_dir,
                        force_retrain=force_retrain)

            # Inference on data sources.
            # Assuming inferences don't get stale, we can build a cache of all
            # previous inference to make sure we don't duplicate work. This is
            # important to make inference on incremental changes in data fast
            # and cost-efficient.

            # Get the names of all the files we have ran inference on.
            prev_infer_fnames = _get_all_inference_fnames(local_model_dir)
            prev_infer_fnames = \
                _inference_fnames_to_original_fnames(prev_infer_fnames)
            # Download them.
            local_infer_fnames = \
                download_files_to_local(prev_infer_fnames, local_data_dir)
            # Build a cache from them.
            inference_cache = \
                build_inference_cache(local_model_dir, local_infer_fnames)

            if len(infer_fnames) > 0:
                local_fnames = download_files_to_local(
                    infer_fnames, local_data_dir)
                for fname in local_fnames:
                    inference(local_model_dir, remote_dir,
                              fname, inference_cache)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Train Model')
    parser.add_argument('--dir', help='Remote GCS dir containing the assets')
    parser.add_argument('--dirs', default=[], nargs='*',
                        help='Remote GCS dirs containing the assets '
                             '(takes precedence over the --dir argument)')
    parser.add_argument('--infer', default=[], nargs='*',
                        help='A list of full gs:// filenames to run inference on')
    parser.add_argument('--force-retrain',
                        action='store_true', help='Force retraining')
    parser.add_argument('--eval-batch-size',
                        type=int, default=8, help='eval_batch_size')
    args = parser.parse_args()

    remote_dir = args.dir
    remote_dirs = args.dirs
    infer_fnames = args.infer
    force_retrain = args.force_retrain
    eval_batch_size = args.eval_batch_size

    if len(remote_dirs) == 0 and remote_dir:
        remote_dirs.append(remote_dir)

    run(remote_dirs, infer_fnames, force_retrain, eval_batch_size)

'''
Try it out locally:

python -m train.gcp_run --dir gs://alchemy-gp/tasks/8a79a035-56fa-415c-8202-9297652dfe75/models/3 --infer gs://alchemy-gp/data/spring_jan_2020_small.jsonl --eval-batch-size 32
'''
