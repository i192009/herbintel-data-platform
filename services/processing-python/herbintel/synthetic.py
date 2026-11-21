import base64
import csv
import io
import json
import random
import time
import zipfile
from pathlib import Path

from docx import Document
from PIL import Image, ImageDraw, ImageFont
from reportlab.pdfgen import canvas


def document(source: str, filename: str, data: bytes, approved: bool = True) -> dict:
    return {
        "source_id": source,
        "source_version": 1,
        "filename": filename,
        "content_base64": base64.b64encode(data).decode(),
        "category": "synthetic_educational",
        "approved": approved,
    }


def text(index: int, seed: int) -> str:
    return (
        f"SYNTHETIC REFERENCE DOCUMENT {index}. Seed {seed}.\n"
        "This fictional reference describes data engineering evaluation of herbal literature metadata.\n"
        "It contains no patient information, dosage instructions or clinical recommendations.\n"
        f"Section: research processing sample number {index}.\n"
    )


def documents(count: int = 1000, seed: int = 42) -> list[dict]:
    if count < 12:
        raise ValueError("representative corpus requires at least 12 documents")
    items = [document(f"synthetic-{i}", f"reference-{i}.txt", text(i, seed).encode()) for i in range(count)]
    # Same bytes, different source: content deduplication must prevent split leakage.
    items[-1] = document("duplicate-content", "duplicate.txt", text(0, seed).encode())
    items[-2] = document("malformed-json", "malformed.json", b'{"text": broken')
    items[-3] = document(
        "chinese-reference",
        "reference-zh.txt",
        "合成参考文档。仅用于数据工程测试，不包含患者信息或医学建议。草药研究数据处理与质量检查。".encode(),
    )
    pdf = io.BytesIO()
    c = canvas.Canvas(pdf, invariant=1)
    c.drawString(40, 780, "Synthetic reference document for native PDF extraction research.")
    c.save()
    items[-4] = document("native-pdf", "native.pdf", pdf.getvalue())
    image = Image.new("RGB", (1600, 450), "white")
    draw = ImageDraw.Draw(image)
    font_path = next(
        (
            p
            for p in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "C:/Windows/Fonts/arial.ttf")
            if Path(p).exists()
        ),
        None,
    )
    font = ImageFont.truetype(font_path, 38) if font_path else ImageFont.load_default()
    draw.text((40, 70), "SYNTHETIC REFERENCE DOCUMENT", fill="black", font=font)
    draw.text((40, 140), "OCR extraction research sample. No patient data.", fill="black", font=font)
    scanned = io.BytesIO()
    image.save(scanned, format="PDF", resolution=150, creationDate=time.gmtime(0), modDate=time.gmtime(0))
    items[-5] = document("scanned-pdf", "scanned.pdf", scanned.getvalue())
    png = io.BytesIO()
    image.save(png, format="PNG")
    items[-6] = document("ocr-image", "image.png", png.getvalue())
    docx = Document()
    docx.add_paragraph(text(count - 7, seed))
    word = io.BytesIO()
    docx.save(word)
    stable = io.BytesIO()
    with zipfile.ZipFile(word) as source, zipfile.ZipFile(stable, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for name in sorted(source.namelist()):
            info = zipfile.ZipInfo(name, date_time=(2025, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            target.writestr(info, source.read(name))
    items[-7] = document("word-reference", "reference.docx", stable.getvalue())
    items[-8] = document("json-reference", "reference.json", json.dumps({"text": text(count - 8, seed)}).encode())
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["text"])
    writer.writerow([text(count - 9, seed)])
    items[-9] = document("csv-reference", "reference.csv", buf.getvalue().encode())
    items[-10] = document("synthetic-1", "updated.txt", (text(1, seed) + "Updated source version.").encode())
    items[-10]["source_version"] = 2
    items[-11] = document("unapproved-reference", "unapproved.txt", text(count - 11, seed).encode(), approved=False)
    return items


def metadata_records(count: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    return [
        {
            "source_id": f"synthetic-{i:06}",
            "source_version": "2" if i % 37 == 0 else "1",
            "document_language": "zh" if i % 11 == 0 else "en",
            "category": "synthetic_educational",
            "event_timestamp": "2025-01-01T00:00:00Z" if i % 29 == 0 else "2025-02-01T00:00:00Z",
            "synthetic_value": rng.randrange(1_000_000),
        }
        for i in range(count)
    ]
