import os
from pathlib import Path
from inference.base import ITextCatModel
from shared.utils import load_jsonl, save_jsonl, mkf, mkd

DEFAULT_INFERENCE_CACHE_STORAGE = '__infcache'

def get_predicted_cached(data_fname, model:ITextCatModel):
    stem = Path(data_fname).stem
    fname = f'{stem}__inferred_by__{model.model_id}.jsonl'
    path = [DEFAULT_INFERENCE_CACHE_STORAGE, fname]
    fname = os.path.join(*path)

    if not os.path.isfile(fname):
        df = load_jsonl(data_fname)
        res = model.predict(df['text'])
        mkf(*path)
        save_jsonl(fname, res)

    print("get_predicted_cached get", fname)
    return load_jsonl(fname, to_df=False)
