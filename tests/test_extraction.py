import io
import json
import zipfile

import pytest
from herbintel.extraction import extract, language, normalize
from herbintel.synthetic import documents, text


@pytest.mark.parametrize(
    "kind,data",
    [
        ("txt", text(1, 42).encode()),
        ("json", json.dumps({"text": text(1, 42)}).encode()),
        ("csv", b'text\n"Synthetic reference document for extraction and research testing."\n'),
    ],
)
def test_native_formats(kind, data):
    result, pages, method, sections = extract(data, kind)
    assert "reference" in result.lower()
    assert pages == len(sections) == 1
    assert method == "native"


@pytest.mark.parametrize(
    "kind,data", [("json", b'{"bad":1}'), ("json", b"{broken"), ("txt", b"\xff"), ("txt", b""), ("csv", b"wrong\nx")]
)
def test_malformed_inputs_are_visible(kind, data):
    with pytest.raises((ValueError, UnicodeError)):
        extract(data, kind)


def test_native_pdf_and_docx():
    import base64

    rows = documents(100)
    for name in ("native.pdf", "reference.docx"):
        row = next(r for r in rows if r["filename"] == name)
        result, pages, method, _ = extract(base64.b64decode(row["content_base64"]), name.split(".")[-1])
        assert len(result) > 30 and pages == 1 and method == "native"


def test_docx_zip_bomb_guard():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("bomb.xml", b"a" * (33 * 1024 * 1024))
    with pytest.raises(ValueError, match="expanded size"):
        extract(stream.getvalue(), "docx")


def test_normalization_and_conservative_language():
    assert normalize("Ａ  reference\x00\r\n text ") == "A reference\ntext"
    assert language("合成参考文档数据测试") == "zh"
    assert language("123456") is None


def test_generator_deterministic():
    from herbintel.synthetic import metadata_records

    assert metadata_records(100, 42) == metadata_records(100, 42)
    assert metadata_records(100, 42) != metadata_records(100, 43)
