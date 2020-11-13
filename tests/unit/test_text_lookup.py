from pathlib import Path

from alchemy.shared.file_adapters import save_jsonl
from alchemy.train.text_lookup import get_entity_text_lookup_function


def test_get_entity_text_lookup_function(monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = Path('__filestore')
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))
    dummy_entity_id = 123
    p = tmp_path / "data.jsonl"
    data = [
        {"text": "hello world", "meta": {"name": "a"}},
        {"text": "lorem ipsum", "meta": {"name": "b"}},
    ]
    save_jsonl(str(p), data, data_store=data_store)

    fn = get_entity_text_lookup_function(str(p), "meta.name", "text", dummy_entity_id, data_store=data_store)

    assert fn(dummy_entity_id, "b") == "lorem ipsum"
    assert fn(dummy_entity_id, "a") == "hello world"
    assert fn(dummy_entity_id, "x") == ""
    assert fn(dummy_entity_id + 1, "a") == ""
    assert fn(dummy_entity_id + 1, "x") == ""
