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


def download_files_to_local(remote_data_dir, fnames, local_data_dir):
    remote_fnames = [f'{remote_data_dir}/{fname}' for fname in fnames]

    local_fnames = []
    for fname in remote_fnames:
        local_fname = os.path.join(local_data_dir, Path(fname).name)
        copy_file(fname, local_fname)
        local_fnames.append(local_fname)
    return local_fnames


def inference(local_dir, remote_dir, local_fname, inference_cache=None):
    # Download a trained model
    copy_dir(remote_dir, local_dir)

    # Run Inference - results saved in local_dir
    _inference(local_dir, local_fname, inference_cache=inference_cache)

    # Upload results
    copy_dir(local_dir, remote_dir)


def run(remote_model_dirs, remote_data_dir, infer_fnames, force_retrain, eval_batch_size):
    """
    For all models in `remote_model_dirs`
        Train if `force_retrain`
        For all files in `infer_fnames`
            Run model inference on those files using bsize `eval_batch_size`
    """
    print("Running Training & Inference")
    print(f"remote_model_dirs={remote_model_dirs}")
    print(f"infer_fnames={infer_fnames}")
    print(f"force_retrain={force_retrain}")
    print(f"eval_batch_size={eval_batch_size}")

    if isinstance(eval_batch_size, int):
        os.environ['TRANSFORMER_EVAL_BATCH_SIZE'] = str(eval_batch_size)

    for remote_model_dir in remote_model_dirs:
        print(f"Executing Training Script")

        with tempfile.TemporaryDirectory() as local_dir:
            # -----------------------------------------------------------------
            # 1. Setup
            local_model_dir = os.path.join(local_dir, 'model')
            local_data_dir = os.path.join(local_dir, 'data')

            os.makedirs(local_model_dir, exist_ok=True)
            os.makedirs(local_data_dir, exist_ok=True)

            # -----------------------------------------------------------------
            # 2. Train model, if needed (unless force_retrain=True)
            train_model(local_model_dir, remote_model_dir,
                        force_retrain=force_retrain)

            # -----------------------------------------------------------------
            # 3. Build a cache of previous inference results

            # Assuming inferences don't get stale, we can build a cache of all
            # previous inference to make sure we don't duplicate work. This is
            # important to make inference on incremental changes in data fast
            # and cost-efficient.

            # TODO only build cache if we have some files for inference?

            # Get the names of all the files we have ran inference on.
            prev_infer_fnames = _get_all_inference_fnames(local_model_dir)
            prev_infer_fnames = \
                _inference_fnames_to_original_fnames(prev_infer_fnames)
            # Download them.
            local_infer_fnames = download_files_to_local(
                remote_data_dir, prev_infer_fnames, local_data_dir)
            # Build a cache from them.
            inference_cache = \
                build_inference_cache(local_model_dir, local_infer_fnames)

            # -----------------------------------------------------------------
            # 3. Run inference on all the files
            if len(infer_fnames) > 0:
                local_fnames = download_files_to_local(
                    remote_data_dir, infer_fnames, local_data_dir)
                for fname in local_fnames:
                    # TODO don't run inference if we have already done it on that file.
                    inference(local_model_dir, remote_model_dir,
                              fname, inference_cache)

            # TODO remove model locally to save space!


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Train Model')
    parser.add_argument('--dirs', default=[], nargs='*',
                        help='Remote GCS dirs containing the assets.')
    parser.add_argument('--data-dir',
                        help='Location of raw data files.')
    parser.add_argument('--infer', default=[], nargs='*',
                        help='A list of filenames to run inference on')
    parser.add_argument('--force-retrain',
                        action='store_true', help='Force retraining')
    parser.add_argument('--eval-batch-size',
                        type=int, default=8, help='eval_batch_size')
    args = parser.parse_args()

    remote_model_dirs = args.dirs
    data_dir = args.data_dir
    infer_fnames = args.infer
    force_retrain = args.force_retrain
    eval_batch_size = args.eval_batch_size

    run(remote_model_dirs, data_dir, infer_fnames, force_retrain, eval_batch_size)

'''
Try it out locally:

python -m train.no_deps.gcp_run --dir gs://alchemy-gp/tasks/8a79a035-56fa-415c-8202-9297652dfe75/models/3 --data-dir gs://alchemy-gp/data --infer spring_jan_2020_small.jsonl --eval-batch-size 32
'''
