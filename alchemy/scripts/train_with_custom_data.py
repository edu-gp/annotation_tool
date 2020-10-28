import pandas as pd

from alchemy.shared.utils import save_jsonl
from alchemy.train import GCPJob, get_model_dir, get_next_version, save_config
from alchemy.train.gcp_celery import poll_status as gcp_poll_status
from alchemy.train.no_deps.paths import _get_exported_data_fname


def train_with_custom_data(
    task_id, custom_data_file, text_col, label_col, label, dryrun=True
):
    """Train with custom data (in a csv file)

    Inputs:
        task_id: Where to store this model under. Also, the inference will run using
            whatever files are listed under this task.
        custom_data_file: A csv file.
        text_col: The text column in custom_data_file.
        label_col: The label column in custom_data_file. The column should be {-1, 0, 1},
            -1 is negative, 0 is unknown (ignore), 1 is positive.
        label: The label name to use (can be whatever you want)
        dryrun: If true, don't actually submit it to GCP for training.
    """
    print("Export labeled examples from user csv (b2c_final_augmented)...")

    version = get_next_version(task_id)
    version_dir = get_model_dir(task_id, version)

    save_config(version_dir)

    data_fname = _get_exported_data_fname(version_dir)
    convert_user_csv(
        custom_data_file,
        text_col=text_col,
        label_col=label_col,
        label=label,
        outfile=data_fname,
    )

    if not dryrun:
        job = GCPJob(task_id, version)
        job.submit()
        gcp_poll_status.delay(id, version)
    else:
        print("Dry run - not submitting job to GCP.")


def convert_user_csv(csv_fname, text_col, label_col, label, outfile):
    df = pd.read_csv(csv_fname)

    data = []
    for idx, row in df.iterrows():
        if (
            row[label_col] != 0
            and not pd.isna(row[text_col])
            and len(row[text_col]) > 0
        ):

            assert row[label_col] in [-1, 1], f'Invalid Label: "{row[label_col]}"'

            data.append({"text": row[text_col], "labels": {label: row[label_col]}})

    save_jsonl(outfile, data)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train with custom data")
    parser.add_argument("--task_id", required=True)
    parser.add_argument("--file", required=True, help="CSV data file")
    parser.add_argument("--text_col", required=True, help="Text col name")
    parser.add_argument(
        "--label_col",
        required=True,
        help="Label col name - labels should be -1, 0, or 1",
    )
    parser.add_argument("--label", required=True, help="Label name")
    parser.add_argument("--dry", default=False, action="store_true", help="Dry run")

    args = parser.parse_args()
    print(args)
    train_with_custom_data(
        args.task_id,
        args.file,
        args.text_col,
        args.label_col,
        args.label,
        dryrun=args.dry,
    )

"""
Example past runs:

python -m scripts.train_with_custom_data \
    --task_id b11e8706-1c9e-4dd0-961e-4ad30a4d9985 \
    --file b2c_high_confidence.csv \
    --text_col req__data__text \
    --label_col 'Majority Vote' \
    --label B2C \
    --dry

python -m scripts.train_with_custom_data \
    --task_id b11e8706-1c9e-4dd0-961e-4ad30a4d9985 \
    --file b2c_final.csv \
    --text_col req__data__text \
    --label_col 'Final' \
    --label B2C \
    --dry

python -m scripts.train_with_custom_data \
    --task_id b11e8706-1c9e-4dd0-961e-4ad30a4d9985 \
    --file b2c_final_augmented.csv \
    --text_col description \
    --label_col label \
    --label B2C \
    --dry

python -m scripts.train_with_custom_data \
    --task_id b11e8706-1c9e-4dd0-961e-4ad30a4d9985 \
    --file mert_grc.csv \
    --text_col Description \
    --label_col Label \
    --label GRC \
    --dry
"""
