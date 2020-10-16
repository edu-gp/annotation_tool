import os

from alchemy.db.model import Model, _raw_data_file_path
from alchemy.shared.utils import save_jsonl

# Note: These are all designed to run on a Celery queue. We can move them when
# the requests are too slow to run on web server.


def export_new_raw_data(
    model: Model, data_fname: str, output_fname: str, cutoff: float = 0.5
):
    """Exports a model prediction in the raw data format.
    Note: This will NOT allow you to overwrite an existing file.
    """

    df = model.export_inference(data_fname, include_text=True)
    df = df[df["probs"] > cutoff]

    if not output_fname.endswith(".jsonl"):
        output_fname += ".jsonl"
    output_fname = output_fname.replace(" ", "_")
    output_path = _raw_data_file_path(output_fname)

    if os.path.isfile(output_path):
        raise Exception(f"Cannot overwrite existing file: {output_path}")

    # Convert to the standard raw data file format
    data = []
    for row in df.itertuples():
        data.append(
            {"text": row.text, "meta": {"name": row.name, "domain": row.domain}}
        )

    save_jsonl(output_path, data)

    return output_path
