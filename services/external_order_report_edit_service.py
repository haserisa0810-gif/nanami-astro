from __future__ import annotations

import copy
from typing import Any

from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import SessionLocal
from models import ExternalOrder, ExternalOrderReportEdit
from services.external_report_template_renderer import chapter_specs


def normalize_chapter_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    upper = raw.upper()
    if upper.startswith("EX"):
        suffix = upper[2:].strip()
        digits = "".join(ch for ch in suffix if ch.isdigit())
        if digits:
            return f"EX{int(digits):02d}"
        return upper
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits:
        return f"{int(digits):02d}"
    return upper


def chapter_section_id(chapter_key: Any) -> str:
    key = normalize_chapter_key(chapter_key)
    if not key:
        return ""
    if key.startswith("EX"):
        suffix = "".join(ch for ch in key[2:] if ch.isdigit())
        if suffix:
            return f"ex{int(suffix):02d}"
        return key.lower()
    if key.isdigit():
        return f"ch{int(key):02d}"
    return key.lower()


def _chapter_html_fragment(body_html: Any) -> str:
    return "" if body_html is None else str(body_html)


def extract_external_order_report_chapters_from_html(html_text: str | None, specs: list[dict[str, str]]) -> list[dict[str, Any]]:
    text = (html_text or "").strip()
    if not text:
        return []

    soup = BeautifulSoup(text, "html.parser")
    out: list[dict[str, Any]] = []
    for spec in specs:
        key = normalize_chapter_key(spec.get("num"))
        section = soup.find("section", id=chapter_section_id(key))
        body_html = ""
        chapter_title = spec.get("title") or ""
        if section:
            title_node = section.select_one(".chapter-title")
            if title_node:
                chapter_title = title_node.get_text(" ", strip=True) or chapter_title
            body_node = section.select_one(".chapter-body")
            if body_node:
                body_html = body_node.decode_contents()
        out.append(
            {
                "chapter_key": key,
                "chapter_title": chapter_title,
                "original_body_html": body_html,
                "manual_body_html": "",
                "display_body_html": body_html,
                "is_manual": False,
                "revision_no": 0,
                "updated_by_type": "",
                "updated_by_id": None,
                "source": "html",
            }
        )
    return out


def _load_edit_map(db: Session, order_id: int) -> dict[str, ExternalOrderReportEdit]:
    rows = db.scalars(
        select(ExternalOrderReportEdit).where(ExternalOrderReportEdit.external_order_id == order_id)
    ).all()
    return {normalize_chapter_key(row.chapter_key): row for row in rows}


def _row_to_card(row: ExternalOrderReportEdit) -> dict[str, Any]:
    display_body_html = row.manual_body_html if row.is_manual and (row.manual_body_html or "").strip() else (row.original_body_html or "")
    return {
        "chapter_key": normalize_chapter_key(row.chapter_key),
        "chapter_title": row.chapter_title,
        "original_body_html": row.original_body_html or "",
        "manual_body_html": row.manual_body_html or "",
        "display_body_html": display_body_html,
        "is_manual": bool(row.is_manual and (row.manual_body_html or "").strip()),
        "revision_no": row.revision_no or 0,
        "updated_at": row.updated_at,
        "updated_by_type": row.updated_by_type or "",
        "updated_by_id": row.updated_by_id,
        "source": "db",
    }


def ensure_external_order_report_edit_rows(
    db: Session,
    order: ExternalOrder,
    *,
    html_text: str | None = None,
    plan: str = "standard",
    report_options: dict[str, bool] | None = None,
) -> list[dict[str, Any]]:
    edit_map = _load_edit_map(db, order.id)
    if edit_map:
        return load_external_order_report_chapters(
            db,
            order,
            html_text=html_text,
            plan=plan,
            report_options=report_options,
        )

    specs = chapter_specs(plan, report_options or {})
    parsed = extract_external_order_report_chapters_from_html(html_text, specs)
    if not parsed:
        return []

    for card in parsed:
        row = ExternalOrderReportEdit(
            external_order_id=order.id,
            chapter_key=card["chapter_key"],
            chapter_title=card["chapter_title"],
            original_body_html=card["original_body_html"],
            manual_body_html=None,
            is_manual=False,
            updated_by_type="system",
            updated_by_id=None,
            revision_no=1,
        )
        db.add(row)
    db.commit()
    return parsed


def load_external_order_report_chapters(
    db: Session,
    order: ExternalOrder,
    *,
    html_text: str | None = None,
    plan: str = "standard",
    report_options: dict[str, bool] | None = None,
) -> list[dict[str, Any]]:
    specs = chapter_specs(plan, report_options or {})
    edit_map = _load_edit_map(db, order.id)
    parsed_map = {item["chapter_key"]: item for item in extract_external_order_report_chapters_from_html(html_text, specs)} if html_text else {}

    cards: list[dict[str, Any]] = []
    for spec in specs:
        key = normalize_chapter_key(spec["num"])
        row = edit_map.get(key)
        if row:
            cards.append(_row_to_card(row))
            continue
        parsed = parsed_map.get(key)
        if parsed:
            cards.append(parsed)
            continue
        cards.append(
            {
                "chapter_key": key,
                "chapter_title": spec["title"],
                "original_body_html": "",
                "manual_body_html": "",
                "display_body_html": "",
                "is_manual": False,
                "revision_no": 0,
                "updated_at": None,
                "updated_by_type": "",
                "updated_by_id": None,
                "source": "missing",
            }
        )
    return cards


def save_external_order_report_edit(
    db: Session,
    *,
    order_id: int,
    chapter_key: Any,
    chapter_title: str,
    original_body_html: str = "",
    manual_body_html: str = "",
    updated_by_type: str = "system",
    updated_by_id: int | None = None,
) -> ExternalOrderReportEdit:
    key = normalize_chapter_key(chapter_key)
    row = db.scalar(
        select(ExternalOrderReportEdit).where(
            ExternalOrderReportEdit.external_order_id == order_id,
            ExternalOrderReportEdit.chapter_key == key,
        )
    )
    if row is None:
        row = ExternalOrderReportEdit(
            external_order_id=order_id,
            chapter_key=key,
            chapter_title=chapter_title,
            original_body_html=original_body_html or "",
            manual_body_html=None,
            is_manual=False,
            updated_by_type=updated_by_type,
            updated_by_id=updated_by_id,
            revision_no=1,
        )
        db.add(row)
    else:
        if chapter_title:
            row.chapter_title = chapter_title
        if original_body_html:
            row.original_body_html = original_body_html
        row.revision_no = int(row.revision_no or 0) + 1
        row.updated_by_type = updated_by_type
        row.updated_by_id = updated_by_id

    manual = (manual_body_html or "").strip()
    if manual:
        row.manual_body_html = manual
        row.is_manual = True
    else:
        row.manual_body_html = None
        row.is_manual = False
    return row


def reset_external_order_report_edit(
    db: Session,
    *,
    order_id: int,
    chapter_key: Any,
    updated_by_type: str = "system",
    updated_by_id: int | None = None,
) -> ExternalOrderReportEdit | None:
    key = normalize_chapter_key(chapter_key)
    row = db.scalar(
        select(ExternalOrderReportEdit).where(
            ExternalOrderReportEdit.external_order_id == order_id,
            ExternalOrderReportEdit.chapter_key == key,
        )
    )
    if not row:
        return None
    row.manual_body_html = None
    row.is_manual = False
    row.updated_by_type = updated_by_type
    row.updated_by_id = updated_by_id
    row.revision_no = int(row.revision_no or 0) + 1
    return row


def replace_chapter_body_in_html(html_text: str, chapter_key: Any, new_body_html: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    section = soup.find("section", id=chapter_section_id(chapter_key))
    if not section:
        raise ValueError(f"chapter section not found: {chapter_key}")
    body_node = section.select_one(".chapter-body")
    if not body_node:
        raise ValueError(f"chapter body not found: {chapter_key}")
    body_node.clear()
    fragment = BeautifulSoup(_chapter_html_fragment(new_body_html), "html.parser")
    root = fragment.body if fragment.body else fragment
    for child in list(root.contents):
        body_node.append(copy.copy(child))
    return str(soup)


def sync_generated_external_order_report_chapters(
    order_id: int,
    chapter_content: dict[str, Any],
    *,
    updated_by_type: str = "system",
    updated_by_id: int | None = None,
) -> dict[str, Any]:
    chapters = chapter_content.get("chapters") if isinstance(chapter_content, dict) else None
    if not isinstance(chapters, list):
        return chapter_content

    with SessionLocal() as db:
        edit_map = _load_edit_map(db, order_id)
        merged_chapters: list[dict[str, Any]] = []
        changed = False
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            key = normalize_chapter_key(chapter.get("num"))
            if not key:
                continue
            title = (chapter.get("title") or "").strip()
            original_body_html = _chapter_html_fragment(chapter.get("body_html"))
            row = edit_map.get(key)
            if row is None:
                row = ExternalOrderReportEdit(
                    external_order_id=order_id,
                    chapter_key=key,
                    chapter_title=title or key,
                    original_body_html=original_body_html,
                    manual_body_html=None,
                    is_manual=False,
                    updated_by_type=updated_by_type,
                    updated_by_id=updated_by_id,
                    revision_no=1,
                )
                db.add(row)
                edit_map[key] = row
                changed = True
            else:
                if title:
                    row.chapter_title = title
                row.original_body_html = original_body_html
                row.updated_by_type = updated_by_type
                row.updated_by_id = updated_by_id
                row.revision_no = int(row.revision_no or 0) + 1
                changed = True

            manual = (row.manual_body_html or "").strip() if row.is_manual else ""
            merged_body = manual or original_body_html
            merged_chapters.append(
                {
                    **chapter,
                    "num": chapter.get("num"),
                    "title": row.chapter_title or title or key,
                    "body_html": merged_body,
                    "original_body_html": original_body_html,
                    "manual_body_html": row.manual_body_html or "",
                    "is_manual": bool(row.is_manual and manual),
                    "chapter_key": key,
                }
            )

        if changed:
            db.commit()

    merged = dict(chapter_content)
    merged["chapters"] = merged_chapters
    return merged
