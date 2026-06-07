from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

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
@page external-report-body {
  size: A4;
  margin: 18mm 16mm 20mm 16mm;
}
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
.report-body-page {
  page: external-report-body;
  break-inside: auto !important;
  page-break-inside: auto !important;
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}
.report-body-page.section,
.report-body-page.chapter {
  break-inside: auto !important;
  page-break-inside: auto !important;
}
.report-body-page > .chapter-inner,
.report-body-page > .section-inner {
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}
.report-body-page .chapter-eyebrow,
.report-body-page .chapter-divider,
.report-body-page .naming {
  break-inside: avoid !important;
  page-break-inside: avoid !important;
}
.report-body-page .chapter-body {
  break-inside: auto !important;
  page-break-inside: auto !important;
}
.report-body-page .chapter-body p {
  break-inside: auto !important;
  page-break-inside: auto !important;
  orphans: 3;
  widows: 3;
}
.report-body-page .closer {
  break-inside: avoid !important;
  page-break-inside: avoid !important;
  margin-top: 2rem !important;
  padding-top: 1.25rem !important;
}
.report-body-page .chapter {
  padding: 3.5rem 2rem 4rem !important;
}
.page-break, .pdf-page-break { break-before: page; page-break-before: always; }
.cover-info,
.cover-info > div,
.cover-info-table {
  width: 680px !important;
  max-width: 680px !important;
  margin-left: auto !important;
  margin-right: auto !important;
}
.cover-info-table {
  display: grid !important;
  grid-template-columns: minmax(6.5em, 9em) minmax(0, 1fr) !important;
  gap: 7px 18px !important;
  text-align: left !important;
  align-items: start !important;
}
.cover-label,
.cover-value,
.cit-label,
.cit-val {
  display: block !important;
  writing-mode: horizontal-tb !important;
  text-orientation: mixed !important;
  white-space: normal !important;
  word-break: normal !important;
  line-height: 1.65 !important;
}
.cover-label,
.cit-label {
  min-width: 6.5em !important;
  white-space: nowrap !important;
  overflow-wrap: normal !important;
  letter-spacing: .08em !important;
}
.cover-value,
.cit-val {
  min-width: 0 !important;
  overflow-wrap: anywhere !important;
}
.cover-info-table.nanami-pdf-cover-info-table {
  display: table !important;
  table-layout: fixed !important;
  border-collapse: separate !important;
  border-spacing: 0 7px !important;
  text-align: left !important;
}
.cover-info-table.nanami-pdf-cover-info-table th,
.cover-info-table.nanami-pdf-cover-info-table td {
  display: table-cell !important;
  writing-mode: horizontal-tb !important;
  text-orientation: mixed !important;
  word-break: normal !important;
  line-height: 1.65 !important;
  vertical-align: top !important;
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}
.cover-info-table.nanami-pdf-cover-info-table th {
  width: 9em !important;
  min-width: 9em !important;
  max-width: 9em !important;
  padding-left: 0 !important;
  padding-right: 18px !important;
  white-space: nowrap !important;
  overflow-wrap: normal !important;
  font-weight: inherit !important;
}
.cover-info-table.nanami-pdf-cover-info-table td {
  width: auto !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
  white-space: normal !important;
  overflow-wrap: anywhere !important;
}
#charts {
  break-inside: auto !important;
  page-break-inside: auto !important;
  padding-top: 42px !important;
  padding-bottom: 42px !important;
}
#charts > .chapter-inner-wide,
#charts > .chart-inner {
  max-width: 720px !important;
  margin-left: auto !important;
  margin-right: auto !important;
}
#charts .chapter-eyebrow {
  margin-bottom: 22px !important;
}
#charts .chapter-divider {
  margin-bottom: 26px !important;
}
#charts .chart-page {
  break-inside: avoid-page !important;
  page-break-inside: avoid !important;
  padding-top: 18px !important;
  padding-bottom: 18px !important;
}
#charts .chart-page + .chart-page {
  break-before: page !important;
  page-break-before: always !important;
}
#charts .chart-section-label {
  margin-top: 0 !important;
  margin-bottom: 18px !important;
  line-height: 1.5 !important;
}
#charts .chart-frame,
#charts .chart-svg-wrap {
  max-width: 540px !important;
  margin-top: 0 !important;
  margin-bottom: 24px !important;
  break-inside: avoid-page !important;
  page-break-inside: avoid !important;
}
#charts .chart-frame svg,
#charts .chart-svg-wrap svg {
  width: 100% !important;
  height: auto !important;
  display: block !important;
}
#charts .shichu-wrap,
#charts .shichu-table,
#charts .element-bars {
  break-inside: avoid-page !important;
  page-break-inside: avoid !important;
}
#charts .shichu-wrap {
  margin-bottom: 28px !important;
  overflow: visible !important;
}
#charts .shichu-table {
  max-width: 580px !important;
  margin-top: 0 !important;
  margin-bottom: 0 !important;
}
#charts .chart-section-label-five {
  margin-top: 30px !important;
}
#charts .element-bars {
  max-width: 560px !important;
  margin: 16px auto 0 !important;
}
</style>
'''
    html = _normalize_cover_info_tables_for_pdf(html)
    html = _normalize_chart_pages_for_pdf(html)
    if "nanami-pdf-export-css" not in html:
        if "</head>" in html:
            return html.replace("</head>", css + "</head>", 1)
        return css + html
    return html


def _normalize_cover_info_tables_for_pdf(html: str) -> str:
    """Convert the cover profile span grid to a real table for WeasyPrint.

    WeasyPrint's CSS grid support is limited enough that the original inline
    grid can shrink to a narrow column, causing Japanese labels to wrap one
    character per line. A table is more predictable for this fixed metadata
    block and still leaves the stored customer-facing HTML unchanged.
    """
    if not html or "cover-info-table" not in html:
        return html
    if "nanami-pdf-cover-info-table" in html:
        return html

    div_re = re.compile(
        r"<div\b(?P<attrs>[^>]*class=(?P<quote>['\"])[^'\"]*\bcover-info-table\b[^'\"]*(?P=quote)[^>]*)>"
        r"(?P<body>.*?)</div>",
        flags=re.I | re.S,
    )
    span_pair_re = re.compile(
        r"<span\b[^>]*class=(?P<lq>['\"])(?P<label_class>[^'\"]*(?:cover-label|cit-label)[^'\"]*)(?P=lq)[^>]*>"
        r"(?P<label>.*?)</span>\s*"
        r"<span\b[^>]*class=(?P<vq>['\"])(?P<value_class>[^'\"]*(?:cover-value|cit-val)[^'\"]*)(?P=vq)[^>]*>"
        r"(?P<value>.*?)</span>",
        flags=re.I | re.S,
    )

    converted = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal converted
        body = match.group("body")
        rows: list[str] = []
        for pair in span_pair_re.finditer(body):
            label_class = pair.group("label_class")
            value_class = pair.group("value_class")
            label = pair.group("label").strip()
            value = pair.group("value").strip()
            rows.append(
                "<tr>"
                f"<th class=\"{label_class}\">{label}</th>"
                f"<td class=\"{value_class}\">{value}</td>"
                "</tr>"
            )
        if not rows:
            return match.group(0)
        converted += 1
        return (
            "<table class=\"cover-info-table nanami-pdf-cover-info-table\">"
            "<tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    normalized = div_re.sub(repl, html)
    if converted:
        print(f"[external_pdf][cover] normalized_cover_info_tables={converted}", flush=True)
    return normalized


def _normalize_chart_pages_for_pdf(html: str) -> str:
    """Add chart-page wrappers to older stored report HTML before PDF export."""
    if not html or "id=\"charts\"" not in html and "id='charts'" not in html:
        return html
    if "chart-page-western" in html or "chart-page-shichu" in html:
        return html

    western_marker = '<div class="chart-section-label">Natal Chart'
    shichu_marker = '<div class="chart-section-label" style="margin-top:2.5rem;">Four Pillars of Destiny'
    if western_marker not in html or shichu_marker not in html:
        return html

    normalized = html.replace(
        western_marker,
        '<div class="chart-page chart-page-western">\n    ' + western_marker,
        1,
    )
    normalized = normalized.replace(
        shichu_marker,
        '</div>\n    <div class="chart-page chart-page-shichu">\n    ' + shichu_marker.replace(' style="margin-top:2.5rem;"', ''),
        1,
    )

    shichu_pos = normalized.find("chart-page-shichu")
    if shichu_pos < 0:
        return html
    close_marker = "\n  </div>\n</section>"
    close_pos = normalized.find(close_marker, shichu_pos)
    if close_pos < 0:
        close_marker = "\n  </div>\n</div>"
        close_pos = normalized.find(close_marker, shichu_pos)
    if close_pos < 0:
        return html

    normalized = normalized[:close_pos] + "\n    </div>" + normalized[close_pos:]
    print("[external_pdf][charts] normalized_legacy_chart_pages=2", flush=True)
    return normalized


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
    print(
        f"[external_pdf][cover] order_code={order_code} "
        f"pdf_css={'yes' if 'nanami-pdf-export-css' in html_for_pdf else 'no'} "
        f"table_normalized={'yes' if 'nanami-pdf-cover-info-table' in html_for_pdf else 'no'} "
        f"cover_label_count={html_for_pdf.count('cover-label') + html_for_pdf.count('cit-label')}",
        flush=True,
    )
    object_name = pdf_object_name(order_code)
    total_started = perf_counter()
    last_mark = total_started

    def log_timing(step: str) -> None:
        nonlocal last_mark
        now = perf_counter()
        print(
            f"[external_pdf][timing] order_code={order_code} step={step} "
            f"elapsed_sec={now - last_mark:.2f} total_sec={now - total_started:.2f}",
            flush=True,
        )
        last_mark = now

    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = Path(tmpdir) / "report.pdf"
        try:
            HTML(string=html_for_pdf, base_url=base_url or os.getcwd()).write_pdf(str(out_path))
        except Exception as exc:
            raise RuntimeError(f"PDFレンダリングに失敗しました: {exc}") from exc
        log_timing("weasyprint_render")

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
        log_timing("cloud_storage_upload")

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
