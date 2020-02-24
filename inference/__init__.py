import os
from pathlib import Path
from inference.base import ITextCatModel
from shared.utils import load_jsonl, save_jsonl, mkf, mkd

DEFAULT_INFERENCE_CACHE_STORAGE = '__infcache'

def _predict(data_fname, model):
    df = load_jsonl(data_fname)
    res = model.predict(df['text'])
    return res

def get_predicted(data_fname, model:ITextCatModel, cache=True):
    print(f"get_predicted (cache={cache}) get: {data_fname}")

    if not cache:
        return _predict(data_fname, model)
    else:
        stem = Path(data_fname).stem
        fname = f'{stem}__inferred_by__{model.model_id}.jsonl'
        path = [DEFAULT_INFERENCE_CACHE_STORAGE, fname]
        fname = os.path.join(*path)

        if not os.path.isfile(fname):
            res = _predict(data_fname, model)
            mkf(*path)
            save_jsonl(fname, res)

        print(f"Reading from cache: {fname}")
        return load_jsonl(fname, to_df=False)
