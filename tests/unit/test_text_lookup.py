from shared.utils import save_jsonl
from train.text_lookup import get_entity_text_lookup_function


def test_get_entity_text_lookup_function(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))
    dummy_entity_id = 123
    p = tmp_path / 'data.jsonl'
    data = [
        {'text': 'hello world', 'meta': {'name': 'a'}},
        {'text': 'lorem ipsum', 'meta': {'name': 'b'}},
    ]
    save_jsonl(str(p), data)

    fn = get_entity_text_lookup_function(
        str(p), 'meta.name', 'text', dummy_entity_id)

    assert fn(dummy_entity_id, 'b') == 'lorem ipsum'
    assert fn(dummy_entity_id, 'a') == 'hello world'
    assert fn(dummy_entity_id, 'x') == ''
    assert fn(dummy_entity_id+1, 'a') == ''
    assert fn(dummy_entity_id+1, 'x') == ''
