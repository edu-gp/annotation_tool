from alchemy.ar.utils import get_ar_id


def test_get_ar_id():
    assert (
        get_ar_id("a/b/c.jsonl", 12)
        == "c1f240b9a8bbeeef43e14a3c40d6a2832f4a1b1607b416aceec9a537"
    )
