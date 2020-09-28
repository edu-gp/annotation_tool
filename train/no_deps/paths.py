import os
from pathlib import Path


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


def _get_metrics_v2_fname(version_dir, threshold):
    threshold = round(threshold, 8)
    return os.path.join(version_dir, f'metrics_v2_threshold={threshold}.p')


def _get_inference_dir(version_dir):
    # Note: We're responsible for creating this dir if it doesn't exist yet.
    dirname = os.path.join(version_dir, 'inference')
    os.makedirs(dirname, exist_ok=True)
    return dirname


def _get_inference_fname(version_dir, data_fname):
    stem = Path(data_fname).stem
    return os.path.join(_get_inference_dir(version_dir), f'{stem}.pred')


def _get_inference_density_plot_fname(version_dir, data_fname, class_name):
    stem = Path(data_fname).stem
    return os.path.join(_get_inference_dir(version_dir),
                        f'{stem}.pred.{class_name}.histogram.png')


def _get_all_plots(version_dir):
    dirname = _get_inference_dir(version_dir)
    res = []
    if os.path.isdir(dirname):
        for f in os.listdir(dirname):
            if f.endswith('.histogram.png'):
                res.append(f'{dirname}/{f}')
    return res


def _get_all_inference_fnames(version_dir):
    dirname = _get_inference_dir(version_dir)
    res = []
    if os.path.isdir(dirname):
        for f in os.listdir(dirname):
            if f.endswith('.pred.npy'):
                res.append(f'{dirname}/{f}')
    return res


def _inference_fnames_to_datasets(fnames):
    # Only retain files with the valid ending, and convert them to .jsonl
    ending = '.pred.npy'

    res = []
    for fname in fnames:
        # Remove any directory prefix.
        fname = Path(fname).name
        if fname.endswith(ending):
            # Swap ending with '.jsonl'
            fname = fname[:-len(ending)] + '.jsonl'
            res.append(fname)

    return res
