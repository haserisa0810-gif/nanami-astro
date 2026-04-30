from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from google.cloud import storage


@dataclass(frozen=True)
class PdfResult:
    bucket_name: str
    object_name: str
    size_bytes: int


def pdf_object_name(order_code: str) -> str:
    safe_code = re.sub(r"[^A-Za-z0-9_.-]+", "_", order_code or "external-order")
    return f"external_reports/{safe_code}/report.pdf"


def pdf_filename(order_code: str, customer_name: str | None = None) -> str:
    name = (customer_name or "鑑定書").strip() or "鑑定書"
    safe_name = re.sub(r'[\\/:*?"<>|\r\n]+', "_", name).strip("._ ") or "鑑定書"
    safe_code = re.sub(r"[^A-Za-z0-9_.-]+", "_", order_code or "order")
    return f"{safe_name}_{safe_code}.pdf"


def _inject_pdf_css(html: str) -> str:
    """Add print/PDF CSS without changing the customer-facing HTML file."""
    css = r'''
<style id="nanami-pdf-export-css">
@page { size: A4; margin: 0; }
html, body {
  margin: 0 !important;
  padding: 0 !important;
  -webkit-print-color-adjust: exact !important;
  print-color-adjust: exact !important;
}
body {
  background: #0f0d0a !important;
}
* {
  -webkit-print-color-adjust: exact !important;
  print-color-adjust: exact !important;
}
.scroll-indicator, .scroll-down, .scroll-cue, .no-print, button, nav {
  display: none !important;
}
a { color: inherit; text-decoration: none; }
img, svg, canvas { max-width: 100%; break-inside: avoid; page-break-inside: avoid; }
section, article, .section, .chapter, .card, .report-section {
  break-inside: avoid;
  page-break-inside: avoid;
}
.page-break, .pdf-page-break { break-before: page; page-break-before: always; }
</style>
'''
    if "nanami-pdf-export-css" in html:
        return html
    if "</head>" in html:
        return html.replace("</head>", css + "</head>", 1)
    return css + html


def generate_pdf_from_html_to_storage(
    *,
    html: str,
    order_code: str,
    bucket_name: str,
    base_url: str | None = None,
) -> PdfResult:
    """Render HTML to PDF and upload it to Cloud Storage.

    Uses WeasyPrint because it is lighter than launching a full Chromium process on Cloud Run.
    Raises RuntimeError with a staff-friendly message on failure.
    """
    if not bucket_name:
        raise RuntimeError("PDF保存先バケットが未設定です。EXTERNAL_REPORTS_BUCKET を確認してください。")
    if not (html or "").strip():
        raise RuntimeError("PDF化するHTMLが空です。先にHTMLを登録または生成してください。")

    try:
        from weasyprint import HTML  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "PDF生成ライブラリ WeasyPrint を読み込めません。requirements.txt と Dockerfile の依存関係を反映して再デプロイしてください。"
        ) from exc

    html_for_pdf = _inject_pdf_css(html)
    object_name = pdf_object_name(order_code)

    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = Path(tmpdir) / "report.pdf"
        try:
            HTML(string=html_for_pdf, base_url=base_url or os.getcwd()).write_pdf(str(out_path))
        except Exception as exc:
            raise RuntimeError(f"PDFレンダリングに失敗しました: {exc}") from exc

        size = out_path.stat().st_size if out_path.exists() else 0
        if size <= 0:
            raise RuntimeError("PDFファイルが作成されませんでした。")

        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            blob.cache_control = "private, no-store"
            blob.content_type = "application/pdf"
            blob.metadata = {"generated_at": datetime.utcnow().isoformat() + "Z"}
            blob.upload_from_filename(str(out_path), content_type="application/pdf")
        except Exception as exc:
            raise RuntimeError(f"PDFのCloud Storage保存に失敗しました: {exc}") from exc

    return PdfResult(bucket_name=bucket_name, object_name=object_name, size_bytes=size)


def pdf_exists(*, bucket_name: str, order_code: str) -> bool:
    if not bucket_name:
        return False
    client = storage.Client()
    return client.bucket(bucket_name).blob(pdf_object_name(order_code)).exists()


def download_pdf_bytes(*, bucket_name: str, order_code: str) -> bytes | None:
    if not bucket_name:
        return None
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(pdf_object_name(order_code))
    if not blob.exists():
        return None
    return blob.download_as_bytes()
