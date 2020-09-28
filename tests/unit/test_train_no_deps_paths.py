from train.no_deps.paths import (
    _inference_fnames_to_datasets
)


def test__inference_fnames_to_datasets():
    fnames = _inference_fnames_to_datasets([
        'blah.pred.npy',
        'blah.blah.pred.npy',
        'invalid_file.txt',  # Invalid files are ignored
        '/home/dir/ok.pred.npy']
    )

    assert fnames == [
        'blah.jsonl',
        'blah.blah.jsonl',
        'ok.jsonl'
    ]
