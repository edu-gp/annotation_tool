from alchemy.shared.utils import save_jsonl
from alchemy.train.text_lookup import get_entity_text_lookup_function
from tests.fixtures import config  # noqa


def test_get_entity_text_lookup_function(config):
    tmp_path = config['ALCHEMY_FILESTORE_DIR']

    dummy_entity_id = 123
    p = tmp_path / "data.jsonl"
    data = [
        {"text": "hello world", "meta": {"name": "a"}},
        {"text": "lorem ipsum", "meta": {"name": "b"}},
    ]
    save_jsonl(str(p), data)

    fn = get_entity_text_lookup_function(str(p), "meta.name", "text", dummy_entity_id)

    assert fn(dummy_entity_id, "b") == "lorem ipsum"
    assert fn(dummy_entity_id, "a") == "hello world"
    assert fn(dummy_entity_id, "x") == ""
    assert fn(dummy_entity_id + 1, "a") == ""
    assert fn(dummy_entity_id + 1, "x") == ""
