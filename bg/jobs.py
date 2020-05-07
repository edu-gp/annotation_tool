from db.model import Model
from shared.utils import save_jsonl


def export_new_raw_data(model: Model, data_fname: str, output_fname: str,
                        cutoff: float = 0.5):
    """Exports a model prediction in the raw data format."""

    df = model.export_inference(data_fname, include_text=True)
    df = df[df['probs'] > cutoff]

    if not output_fname.endswith('.jsonl'):
        output_fname += '.jsonl'

    # Convert to the standard raw data file format
    data = []
    for row in df.itertuples():
        data.append({
            "text": row.text,
            "meta": {
                "name": row.name,
                "domain": row.domain
            }
        })

    save_jsonl(output_fname, data)
    return output_fname
