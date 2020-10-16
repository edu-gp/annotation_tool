from alchemy.db.model import AnnotationGuide


def test_plaintext_to_html():
    plaintext = "hello"
    res = AnnotationGuide.plaintext_to_html(plaintext)
    assert res == "hello"

    plaintext = "hello\nworld"
    res = AnnotationGuide.plaintext_to_html(plaintext)
    assert res == "hello<br />world"

    plaintext = ""
    res = AnnotationGuide.plaintext_to_html(plaintext)
    assert res == ""
