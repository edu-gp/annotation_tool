import tempfile
from shared.utils import load_jsonl
from db.utils import get_all_data_files
from train.gs_url import (
    build_raw_data_url, build_model_inference_url, build_prod_inference_url,
    build_prod_metadata_url
)
from train.no_deps.utils import gs_copy_file, gs_exists
from train.no_deps.inference_results import InferenceResults


class DeployedInferenceMetadata:
    def __init__(self, timestamp, model_uuid, model_version,
                 label, threshold, dataset_name):
        self.timestamp = timestamp
        self.model_uuid = model_uuid
        self.model_version = model_version
        self.label = label
        self.threshold = threshold
        self.dataset_name = dataset_name

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'model_uuid': self.model_uuid,
            'model_version': self.model_version,
            'label': self.label,
            'threshold': self.threshold,
            'dataset_name': self.dataset_name
        }

    @classmethod
    def from_dict(cls, obj):
        return cls(
            timestamp=obj.get('timestamp'),
            model_uuid=obj.get('model_uuid'),
            model_version=obj.get('model_version'),
            label=obj.get('label'),
            threshold=obj.get('threshold'),
            dataset_name=obj.get('dataset_name')
        )


def ensure_file_exists_locally(dataset_name: str) -> None:
    """Ensure data `dataset_name` from GCS is available locally.
    Inputs:
        dataset_name: A data dataset_name e.g. "jan_2020.jsonl"
    Raises:
        Exception if the file could not be present locally.
    """
    # TODO test
    # TODO does this belong here?
    # Ensure the file exists locally
    if dataset_name not in get_all_data_files():
        # TODO consolidate these paths
        from db.utils import get_local_data_file_path
        remote_fname = build_raw_data_url(dataset_name)
        local_fname = get_local_data_file_path(dataset_name)
        gs_copy_file(remote_fname, local_fname)

    if dataset_name not in get_all_data_files():
        raise Exception(
            f"Dataset {dataset_name} either does not exist or is invalid")


def has_model_inference(model_uuid, model_version, dataset_name) -> bool:
    """Check if `model` already has ran inference on `dataset_name` REMOTELY on GCS.
    Inputs:
        model_uuid: -
        model_version: -
        dataset_name: A dataset name e.g. "jan_2020.jsonl"
    """
    # TODO test
    url = build_model_inference_url(model_uuid, model_version, dataset_name)
    return gs_exists(url)


def create_deployed_inference(metadata: DeployedInferenceMetadata) -> None:
    """Creates files in the production GCS bucket if the files are there.
    If files are created/updated, it also fires a pubsub message.

    Note: This function only relies on GCS; no dependency on the database.
    Note: This function is idempotent.

    Raises:
        Exception if the model have not ran inference on this file yet.
    """
    # TODO test

    uuid = metadata.model_uuid
    version = metadata.model_version
    dname = metadata.dataset_name
    ts = metadata.timestamp

    # Make sure the model already has the inference ready.
    if not has_model_inference(uuid, version, dname):
        raise Exception(f"Inference result not present on GCS, "
                        f"model_uuid={uuid}, model_version={version}, dataset_name={dname}")

    raw_url = build_raw_data_url(dname)
    inference_url = build_model_inference_url(uuid, version, dname)
    prod_inference_url = build_prod_inference_url(uuid, version, dname, ts)
    prod_metadata_url = build_prod_metadata_url(uuid, version, dname, ts)

    # Do everything on a temporary folder, then upload it to GCS.
    with tempfile.TemporaryDirectory() as tmpdirname:
        # The raw data file and prediction file on GCS
        raw_fname = tmpdirname + '/raw.jsonl'
        pred_fname = tmpdirname + '/pred.npy'
        # Where to write the results locally
        csv_fname = tmpdirname + '/final.csv'
        metadata_fname = tmpdirname + '/metadata.json'

        gs_copy_file(raw_url, raw_fname, no_clobber=False)
        gs_copy_file(inference_url, pred_fname, no_clobber=False)

        df = build_prod_inference_dataframe(
            pred_fname, raw_fname, metadata.threshold)
        df.to_csv(csv_fname, index=False)

        import json
        with open(metadata_fname, 'w') as f:
            f.write(json.dumps(metadata.to_dict()))

        gs_copy_file(csv_fname, prod_inference_url, no_clobber=False)
        gs_copy_file(metadata_fname, prod_metadata_url, no_clobber=False)

    publish_message()


def build_prod_inference_dataframe(pred_fname, raw_fname, threshold):
    inf = InferenceResults.load(pred_fname)

    raw = load_jsonl(raw_fname, to_df=True)

    if len(inf.probs) != len(raw):
        raise Exception(f"Prediction and raw files are different in length"
                        f", metadata={metadata}")

    df = raw

    # Flatten the 'meta' column
    if 'meta' in df and len(df) > 0:
        keys = df['meta'].iloc[0].keys()
        for key in keys:
            df['meta_' + key] = df['meta'].apply(lambda row: row[key])
        df = df.drop(columns=['meta'])

    df['prob'] = inf.probs
    df['pred'] = df['prob'] > threshold

    return df


def publish_message():
    # TODO pubsub that Data Platform can listen to.
    pass
