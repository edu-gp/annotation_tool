import os
from pathlib import Path
from shared.utils import mkd
from db import _task_dir

def _get_all_model_versions(task_id):
    _dir = os.path.join(_task_dir(task_id), 'models')
    if os.path.isdir(_dir):
        versions = []
        for dirname in os.listdir(_dir):
            try:
                versions.append(int(dirname))
            except:
                pass
        return versions
    else:
        return []

def _get_latest_model_version(task_id):
    versions = _get_all_model_versions(task_id)
    if len(versions) > 0:
        return max(versions)
    else:
        return 0

def _get_version_dir(task_id, version):
    return mkd(_task_dir(task_id), 'models', str(version))

def _get_inference_dir(task_id, version):
    return mkd(_task_dir(task_id), 'models', str(version), 'inference')

def _get_inference_fname(task_id, version, data_fname):
    stem = Path(data_fname).stem
    return os.path.join(_get_inference_dir(task_id, version), f'{stem}.pred')
    return inf_fname

def _get_inference_density_plot_fname(task_id, version, data_fname, class_name):
    stem = Path(data_fname).stem
    return os.path.join(_get_inference_dir(task_id, version),
                        f'{stem}.pred.{class_name}.histogram.png')

def _get_all_plots(task_id, version):
    dirname = _get_inference_dir(task_id, version)
    res = []
    if os.path.isdir(dirname):
        for f in os.listdir(dirname):
            if f.endswith('.histogram.png'):
                res.append(f'{dirname}/{f}')
    return res

def _get_model_output_dir(version_dir):
        return os.path.join(version_dir, 'model')

def _get_config_fname(version_dir):
    return os.path.join(version_dir, 'config.json')

def _get_exported_data_fname(version_dir):
    return os.path.join(version_dir, 'data.jsonl')

def _get_data_parser_fname(version_dir):
    return os.path.join(version_dir, 'data_parser.json')

def _get_metrics_fname(version_dir):
    return os.path.join(version_dir, 'metrics.json')
    