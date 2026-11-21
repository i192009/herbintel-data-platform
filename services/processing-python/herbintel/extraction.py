import csv
import io
import json
import re
import subprocess
import tempfile
import unicodedata
import zipfile
from pathlib import Path


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).replace("\x00", "")
    return "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()).strip()


def language(text: str) -> str | None:
    # Deliberately conservative script detection; do not invent a language.
    if len(re.findall(r"[\u4e00-\u9fff]", text)) >= 5:
        return "zh"
    if re.search(r"\b(the|synthetic|reference|document|research)\b", text, re.I):
        return "en"
    return None


def ocr(path: Path) -> str:
    return subprocess.run(
        ["tesseract", str(path), "stdout", "-l", "eng+chi_sim"],
        check=True,
        capture_output=True,
        timeout=45,
    ).stdout.decode("utf-8")


def extract(data: bytes, source_type: str) -> tuple[str, int, str, list[str]]:
    if len(data) > 8 * 1024 * 1024:
        raise ValueError("oversized source")
    pages: list[str]
    method = "native"
    if source_type == "pdf":
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data), strict=True)
        if reader.is_encrypted or not 0 < len(reader.pages) <= 100:
            raise ValueError("encrypted, empty or oversized PDF")
        pages = []
        with tempfile.TemporaryDirectory() as tmp:
            pdf = Path(tmp) / "source.pdf"
            pdf.write_bytes(data)
            for index, page in enumerate(reader.pages):
                text = page.extract_text() or ""
                if len(text.strip()) < 20:
                    if len(reader.pages) > 20:
                        raise ValueError("OCR page limit exceeded")
                    prefix = Path(tmp) / f"page-{index}"
                    subprocess.run(
                        [
                            "pdftoppm",
                            "-f",
                            str(index + 1),
                            "-l",
                            str(index + 1),
                            "-singlefile",
                            "-scale-to",
                            "2400",
                            "-png",
                            str(pdf),
                            str(prefix),
                        ],
                        check=True,
                        capture_output=True,
                        timeout=45,
                    )
                    text = ocr(prefix.with_suffix(".png"))
                    method = "native+ocr"
                pages.append(text)
    elif source_type in ("png", "jpg", "jpeg"):
        from PIL import Image

        Image.MAX_IMAGE_PIXELS = 25_000_000
        with Image.open(io.BytesIO(data)) as im:
            im.verify()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / f"image.{source_type}"
            path.write_bytes(data)
            pages = [ocr(path)]
        method = "ocr"
    elif source_type == "docx":
        from docx import Document

        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            if sum(z.file_size for z in archive.infolist()) > 32 * 1024 * 1024:
                raise ValueError("DOCX expanded size exceeded")
        pages = ["\n".join(p.text for p in Document(io.BytesIO(data)).paragraphs)]
    elif source_type == "json":
        value = json.loads(data.decode("utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("text"), str):
            raise ValueError("JSON requires a text string")
        pages = [value["text"]]
    elif source_type == "csv":
        rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
        if not rows or "text" not in rows[0]:
            raise ValueError("CSV requires text column and rows")
        pages = ["\n".join(row["text"] for row in rows)]
    elif source_type == "txt":
        pages = [data.decode("utf-8", errors="strict")]
    else:
        raise ValueError("unsupported source type")
    normalized = [normalize(p) for p in pages]
    text = normalize("\n\n".join(normalized))
    if len(text) < 20:
        raise ValueError("insufficient extracted text")
    return text, len(pages), method, normalized
