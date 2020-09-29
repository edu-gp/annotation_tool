# This is the entrypoint to an AI Platform job.

import os
import tempfile
from pathlib import Path
from .run import train_model, inference, build_inference_cache, InferenceCache
from .storage_manager import ModelStorageManager, DatasetStorageManager


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

        with tempfile.TemporaryDirectory() as tempdir:
            # -----------------------------------------------------------------
            # 1. Setup
            local_model_dir = os.path.join(tempdir, 'model')
            local_data_dir = os.path.join(tempdir, 'data')

            os.makedirs(local_model_dir, exist_ok=True)
            os.makedirs(local_data_dir, exist_ok=True)

            dsm = DatasetStorageManager(remote_data_dir, local_data_dir)
            msm = ModelStorageManager(remote_model_dir, local_model_dir)

            # Download config and data, and any previously trained model
            msm.download()

            # -----------------------------------------------------------------
            # 2. Train model, if needed (unless force_retrain=True)

            # Train Model
            train_model(msm.local_dir, force_retrain=force_retrain)

            # Upload trained model
            msm.upload()

            # -----------------------------------------------------------------
            # 3. Run inference on all the files

            inference_cache: InferenceCache = None

            if len(infer_fnames) > 0:
                # Make sure the datasets are names and not paths
                infer_fnames = [Path(dataset).name for dataset in infer_fnames]

                for dataset in infer_fnames:
                    dataset_local_path = dsm.download(dataset)

                    # Build an inference_cache, as needed.
                    # Cache is not nessesary but makes inference on incremental
                    # data updates a lot faster.
                    if inference_cache is None:
                        inference_cache = \
                            build_inference_cache(msm.local_dir, dsm)

                    # Run Inference - results saved in local_model_dir
                    # Note the inference_cache is updated for each new file.
                    inference(msm.local_dir, dataset_local_path,
                              inference_cache=inference_cache)

            # Upload inference results
            msm.upload()


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
