import os
import tempfile
import subprocess
import shlex
from pathlib import Path
from .no_deps.run import (
    train_model as _train_model,
    inference as _inference
)


def copy_dir(src_dir, dst_dir):
    # cmd = shlex.split(f'gsutil -m cp -r {src_dir}/* {dst_dir}')
    cmd = shlex.split(f'gsutil -m rsync -r {src_dir} {dst_dir}')
    subprocess.run(cmd)


def copy_file(fname, dst):
    cmd = shlex.split(f'gsutil cp {fname} {dst}')
    subprocess.run(cmd)


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


def inference(local_dir, remote_dir, local_fnames):
    # Download a trained model
    copy_dir(remote_dir, local_dir)

    # Run Inference - results saved in local_dir
    _inference(local_dir, local_fnames)

    # Upload results
    copy_dir(local_dir, remote_dir)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Train Model')
    parser.add_argument('--dir', help='Remote GCS dir containing the assets')
    parser.add_argument('--infer', default=[], nargs='*',
                        help='A list of full gs:// filenames to run inference on')
    parser.add_argument('--force-retrain',
                        action='store_true', help='Force retraining')
    parser.add_argument('--eval-batch-size',
                        type=int, default=8, help='eval_batch_size')
    args = parser.parse_args()

    remote_dir = args.dir
    infer_fnames = args.infer
    force_retrain = args.force_retrain
    eval_batch_size = args.eval_batch_size

    print("Hello from Training Script!")
    print(f"remote_dir={remote_dir}")
    print(f"infer_fnames={infer_fnames}")
    print(f"force_retrain={force_retrain}")
    print(f"eval_batch_size={eval_batch_size}")

    if isinstance(eval_batch_size, int):
        os.environ['TRANSFORMER_EVAL_BATCH_SIZE'] = str(eval_batch_size)

    if remote_dir is not None:
        print(f"Executing Training Script")

        with tempfile.TemporaryDirectory() as local_dir:
            # TODO eddie debug
            local_dir = '/tmp/blah'

            local_model_dir = os.path.join(local_dir, 'model')
            local_data_dir = os.path.join(local_dir, 'data')

            os.makedirs(local_model_dir, exist_ok=True)
            os.makedirs(local_data_dir, exist_ok=True)

            train_model(local_model_dir, remote_dir,
                        force_retrain=force_retrain)

            if len(infer_fnames) > 0:
                local_fnames = download_files_to_local(
                    infer_fnames, local_data_dir)
                inference(local_model_dir, remote_dir, local_fnames)


'''
Try it out locally:

python -m train.gcp_run --dir gs://alchemy-gp/tasks/8a79a035-56fa-415c-8202-9297652dfe75/models/3 --infer gs://alchemy-gp/data/spring_jan_2020_small.jsonl --eval-batch-size 32
'''
