from train.gs_utils import build_prod_inference_dataframe
from train.no_deps.inference_results import InferenceResults
from shared.utils import save_jsonl


def test_build_prod_inference_dataframe(tmp_path):
    pred_path = str(tmp_path / 'valid.npy')
    InferenceResults([[20, 20], [-10, 20]]).save(pred_path)

    raw_path = str(tmp_path / 'raw.jsonl')
    data = [
        {'text': 'hello world', 'meta': {'domain': 'a.com', 'name': 'Entity A'}},
        {'text': 'goodbye world', 'meta': {'domain': 'z.com', 'name': 'Entity Z'}}
    ]
    save_jsonl(raw_path, data)

    df = build_prod_inference_dataframe(pred_path, raw_path, 0.9)

    assert df.iloc[0]['text'] == 'hello world'
    assert df.iloc[0]['meta_domain'] == 'a.com'
    assert df.iloc[0]['meta_name'] == 'Entity A'
    assert abs(df.iloc[0]['prob'] - 0.5) < 0.0001
    assert df.iloc[0]['pred'] == False

    assert df.iloc[1]['text'] == 'goodbye world'
    assert df.iloc[1]['meta_domain'] == 'z.com'
    assert df.iloc[1]['meta_name'] == 'Entity Z'
    assert abs(df.iloc[1]['prob'] - 1.0) < 0.0001
    assert df.iloc[1]['pred'] == True
