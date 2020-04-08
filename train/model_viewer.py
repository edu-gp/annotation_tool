import os
from .paths import (
    _get_all_model_versions, _get_version_dir
)
from .no_deps.paths import (
    _get_config_fname, _get_data_parser_fname, _get_metrics_fname,
    _get_all_plots, _get_exported_data_fname, _get_all_inference_fnames
)
from .no_deps.inference_results import InferenceResults
from shared.utils import load_json, load_jsonl, stem as _stem
from db import _data_dir


class ModelViewer:
    def __init__(self, task_id, version):
        self.task_id = task_id
        self.version = version

    def __str__(self):
        return f'Model v{self.version}'

    @staticmethod
    def fetch_all_for_task(task_id):
        res = []
        for version in sorted(_get_all_model_versions(task_id)):
            res.append(ModelViewer(task_id, version))
        return res

    def _load_json(self, fname_fn):
        version_dir = _get_version_dir(self.task_id, self.version)
        fname = fname_fn(version_dir)
        if os.path.isfile(fname):
            return load_json(fname)
        else:
            return None

    def get_metrics(self):
        return self._load_json(_get_metrics_fname)

    def get_config(self):
        return self._load_json(_get_config_fname)

    def get_data_parser(self):
        return self._load_json(_get_data_parser_fname)

    def get_plots(self):
        """Return a list of urls for plots"""
        version_dir = _get_version_dir(self.task_id, self.version)
        return _get_all_plots(version_dir)

    def get_inference_fnames(self):
        version_dir = _get_version_dir(self.task_id, self.version)
        fnames = _get_all_inference_fnames(version_dir)
        return fnames

    def get_inference_fnames_and_stems(self):
        return [(fname, _stem(fname)) for fname in self.get_inference_fnames()]

    def create_exported_dataframe(self, inference_fname):
        """
        Inputs:
            inference_fname: The full path to the inference filename.
        """
        # Load Inference Results
        ir = InferenceResults.load(inference_fname)

        # Load Original Data
        data_fname = os.path.join(
            _data_dir(), _stem(inference_fname) + '.jsonl')
        df = load_jsonl(data_fname, to_df=True)

        # Check they're the same size
        assert len(df) == len(ir.probs)

        # Combine the two together.
        df['probs'] = ir.probs
        df['domain'] = df['meta'].apply(lambda x: x.get('domain'))
        df['name'] = df['meta'].apply(lambda x: x.get('name'))
        # Note: We don't keep the 'text' column on purpose!
        df = df[['name', 'domain', 'probs']]

        return df

    def get_len_data(self):
        """Return how many datapoints were used to train this model"""
        version_dir = _get_version_dir(self.task_id, self.version)
        fname = _get_exported_data_fname(version_dir)

        try:
            import subprocess
            import re
            res = subprocess.run(['wc', '-l', fname], capture_output=True)
            res = res.stdout.decode()
            # `wc -l` returns output in the format of `<num> <filename>`
            # only parse the `num` piece as an int.
            res = int(re.match(r"^\s*(\d+?)\s+.*$", res).groups()[0])
        except Exception as e:
            raise e
            res = -1

        return res

