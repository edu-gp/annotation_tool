from train.no_deps.paths import (
    _inference_fnames_to_original_fnames
)


def test__inference_fnames_to_original_fnames():
    fnames = _inference_fnames_to_original_fnames([
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
