from alchemy.shared.file_adapters import load_jsonl
from alchemy.shared.utils import json_lookup


def get_entity_text_lookup_function(
    jsonl_file_path, entity_name_key, entity_text_key, entity_type, data_store
):
    """
    Inputs:
        jsonl_file_path: Path to a jsonl file
        entity_name_key: Key of the entity name
        entity_text_key: Key of the text
        entity_type: Limit to only this type of entity.

    Returns:
        A function of signature (entity_type, entity_name)
        which returns the entity text if found, else return ''

    Example:
    `jsonl_file_path` points to a file where each line is of the format:

        {
            'text': 'A search engine',
            'meta': {
                'domain': 'google.com',
            }
        }

    Then if:

        entity_name_key = 'meta.domain'
        entity_text_key = 'text'
        entity_type = 'Company'

    Then the resulting lookup function `fn` would behave like:

        fn(1, 'google.com')  =>  'A search engine'
        fn(1, 'blah.com')  =>  ''
    """

    data = load_jsonl(jsonl_file_path, to_df=False, data_store=data_store)

    lookup = {}

    for row in data:
        name = json_lookup(row, entity_name_key)
        if name:
            text = json_lookup(row, entity_text_key) or ""
            lookup[name] = text

    def fn(_entity_type, _entity_name):
        if entity_type == _entity_type:
            return lookup.get(_entity_name, "")
        else:
            return ""

    return fn
