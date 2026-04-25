from __future__ import annotations

import html
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from services.result_builder import _chart_svg  # existing static SVG renderer
from services.shichu_formatter import extract_shichu_data

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"

CHAPTER_TITLES_W_SHICHU = [
    ("01", "基本性格・本質"),
    ("02", "才能・適性・仕事運"),
    ("03", "恋愛・パートナーシップ"),
    ("04", "お金・豊かさの流れ"),
    ("05", "魂の課題・人生のテーマ"),
    ("06", "生涯年表・大運の流れ"),
    ("07", "今年・来年の運勢"),
]

CHAPTER_TITLES_PREMIUM = [
    ("01", "基本性格・本質"),
    ("02", "才能・適性・仕事運"),
    ("03", "恋愛・パートナーシップ"),
    ("04", "お金・豊かさの流れ"),
    ("05", "ルーツ・家族・受け継いだもの"),
    ("06", "カルマと魂の設計図"),
    ("07", "ナクシャトラ — 月の宿が語る本質"),
    ("08", "ダシャー — 三システム統合時期読み"),
    ("09", "体・健康・エネルギー管理"),
    ("10", "特殊星・隠れた才能"),
    ("11", "三システム統合まとめ"),
    ("12", "魂の課題・人生のテーマ"),
    ("13", "今年・来年の運勢"),
]

OPTION_EX_TITLES = {
    "option_asteroids": "小惑星リーディング",
    "option_transit": "トランジット時期読み",
    "option_special_points": "特殊星・隠れた才能",
    "option_year_forecast": "今年・来年の運勢",
}


def _template_css(filename: str) -> str:
    path = TEMPLATE_DIR / filename
    text = path.read_text(encoding="utf-8")
    m = re.search(r"<style>(.*?)</style>", text, flags=re.S | re.I)
    return m.group(1).strip() if m else ""


def _safe(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _jp_date(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return f"{value.year}年{value.month}月{value.day}日"
    raw = str(value or "").strip()
    try:
        d = date.fromisoformat(raw[:10])
        return f"{d.year}年{d.month}月{d.day}日"
    except Exception:
        return raw or "未入力"


def _birth_time_place(order: Any) -> str:
    t = (getattr(order, "birth_time", None) or "").strip() or "時刻不明"
    place = (getattr(order, "birth_place", None) or getattr(order, "prefecture", None) or "").strip()
    if place:
        return f"{_safe(t)}（{_safe(place)}）"
    return _safe(t)


def _deep_get(data: Any, *keys: str) -> Any:
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _planet_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
    western = raw.get("western") if isinstance(raw.get("western"), dict) else raw
    for key in ("planets", "planet_list"):
        val = western.get(key) if isinstance(western, dict) else None
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def _planet_label(raw: dict[str, Any], planet_name: str) -> str:
    for p in _planet_items(raw):
        name = str(p.get("name") or p.get("planet") or "").lower()
        if name == planet_name.lower():
            sign = p.get("sign") or p.get("sign_name") or p.get("zodiac") or ""
            house = p.get("house") or p.get("house_no") or ""
            deg = p.get("deg_in_sign") or p.get("degree") or p.get("degree_in_sign")
            deg_s = ""
            try:
                deg_s = f" {float(deg):.0f}度"
            except Exception:
                pass
            house_s = f"・{house}ハウス" if house else ""
            return f"{sign}{deg_s}{house_s}".strip() or "-"
    return "-"


def _asc_label(raw: dict[str, Any]) -> str:
    candidates = [
        _deep_get(raw, "western", "angles", "asc"),
        _deep_get(raw, "western", "asc"),
        _deep_get(raw, "western", "ASC"),
        raw.get("asc") if isinstance(raw, dict) else None,
    ]
    for c in candidates:
        if isinstance(c, dict):
            sign = c.get("sign") or c.get("sign_name") or ""
            deg = c.get("deg_in_sign") or c.get("degree")
            try:
                return f"{sign} {float(deg):.0f}度".strip()
            except Exception:
                return str(sign or "-")
        if c:
            return str(c)
    return "-"


def _shichu_data(raw: dict[str, Any]) -> dict[str, Any]:
    try:
        return extract_shichu_data(raw)
    except Exception:
        return {"exists": False}


def _shichu_pillars_text(shichu: dict[str, Any]) -> str:
    pillars = shichu.get("pillars") if isinstance(shichu.get("pillars"), dict) else {}
    parts = []
    for key in ["year", "month", "day", "hour"]:
        p = pillars.get(key) if isinstance(pillars.get(key), dict) else {}
        stem = p.get("stem") or ""
        branch = p.get("branch") or ""
        if stem or branch:
            parts.append(f"{stem}{branch}")
    return "・".join(parts) or "-"


def _render_shichu_table(shichu: dict[str, Any], premium: bool = False) -> str:
    pillars = shichu.get("pillars") if isinstance(shichu.get("pillars"), dict) else {}
    hidden = shichu.get("hidden_stems") if isinstance(shichu.get("hidden_stems"), dict) else {}
    keys = ["hour", "day", "month", "year"]
    labels = {"hour": "時柱", "day": "日柱", "month": "月柱", "year": "年柱"}
    def cell(key: str, field: str) -> str:
        p = pillars.get(key) if isinstance(pillars.get(key), dict) else {}
        return _safe(p.get(field) or "-")
    def hidden_cell(key: str) -> str:
        v = hidden.get(key)
        if isinstance(v, list):
            return _safe("・".join(str(x) for x in v if x)) or "-"
        return _safe(v or "-")
    stem_cls = "stem" if premium else "shichu-kanshi"
    branch_cls = "branch" if premium else "shichu-kanshi"
    table_cls = "shichu-table"
    rows = [
        "<div class='shichu-wrap'><table class='shichu-table'><thead><tr>" + "".join(f"<th>{labels[k]}</th>" for k in keys) + "</tr></thead><tbody>",
        "<tr>" + "".join(f"<td><span class='{stem_cls}'>{cell(k,'stem')}</span></td>" for k in keys) + "</tr>",
        "<tr>" + "".join(f"<td><span class='{branch_cls}'>{cell(k,'branch')}</span></td>" for k in keys) + "</tr>",
        "<tr>" + "".join(f"<td class='jingod'><span class='shichu-ten-god'>{hidden_cell(k)}</span></td>" for k in keys) + "</tr>",
        "</tbody></table></div>",
    ]
    return "".join(rows)


def _to_int_count(value: Any) -> int:
    """五行カウントの形式ゆれを吸収する。"""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return 0
        try:
            return int(float(raw))
        except Exception:
            return 0
    if isinstance(value, dict):
        for key in ("count", "value", "total", "score", "n", "amount"):
            if key in value:
                return _to_int_count(value.get(key))
        for v in value.values():
            n = _to_int_count(v)
            if n:
                return n
        return 0
    if isinstance(value, (list, tuple)):
        nums = [_to_int_count(x) for x in value]
        total = sum(nums)
        return total if total else len([x for x in value if x])
    return 0


def _normalize_five_elements(source: Any) -> dict[str, int]:
    """木火土金水の辞書へ正規化する。"""
    result = {"木": 0, "火": 0, "土": 0, "金": 0, "水": 0}
    if not isinstance(source, dict):
        return result

    aliases = {
        "木": ("木", "wood", "moku"),
        "火": ("火", "fire", "ka"),
        "土": ("土", "earth", "do", "soil"),
        "金": ("金", "metal", "kin"),
        "水": ("水", "water", "sui"),
    }
    lower_map = {str(k).strip().lower(): v for k, v in source.items()}
    for jp, keys in aliases.items():
        for key in keys:
            if key in source:
                result[jp] = _to_int_count(source.get(key))
                break
            lk = key.lower()
            if lk in lower_map:
                result[jp] = _to_int_count(lower_map.get(lk))
                break
    return result


def _deep_find_five_elements(data: Any) -> dict[str, int]:
    """よくある場所から五行データを探す。"""
    empty = {"木": 0, "火": 0, "土": 0, "金": 0, "水": 0}
    if not isinstance(data, dict):
        return empty

    candidate_keys = (
        "five_elements",
        "five_element_counts",
        "elements",
        "element_counts",
        "gogyo",
        "五行",
        "五行バランス",
    )

    def walk(obj: Any, depth: int = 0) -> dict[str, int]:
        if depth > 5 or not isinstance(obj, dict):
            return empty.copy()

        for key in candidate_keys:
            val = obj.get(key)
            if isinstance(val, dict):
                counts = _normalize_five_elements(val)
                if any(counts.values()):
                    return counts

        counts = _normalize_five_elements(obj)
        if any(counts.values()):
            return counts

        for v in obj.values():
            found = walk(v, depth + 1)
            if any(found.values()):
                return found

        return empty.copy()

    return walk(data)


def _derive_five_elements_from_pillars(shichu: dict[str, Any]) -> dict[str, int]:
    """五行データが無い時、四柱の天干・地支から簡易カウントする。"""
    counts = {"木": 0, "火": 0, "土": 0, "金": 0, "水": 0}
    mapping = {
        "甲": "木", "乙": "木", "寅": "木", "卯": "木",
        "丙": "火", "丁": "火", "巳": "火", "午": "火",
        "戊": "土", "己": "土", "辰": "土", "戌": "土", "丑": "土", "未": "土",
        "庚": "金", "辛": "金", "申": "金", "酉": "金",
        "壬": "水", "癸": "水", "亥": "水", "子": "水",
    }
    pillars = shichu.get("pillars") if isinstance(shichu.get("pillars"), dict) else {}
    for key in ("year", "month", "day", "hour"):
        p = pillars.get(key) if isinstance(pillars.get(key), dict) else {}
        for field in ("stem", "branch"):
            value = str(p.get(field) or "").strip()
            for ch in value:
                el = mapping.get(ch)
                if el:
                    counts[el] += 1
    return counts


def _render_five_elements(shichu: dict[str, Any], raw: dict[str, Any] | None = None) -> str:
    # 1) formatterが返した五行
    five = _deep_find_five_elements(shichu)

    # 2) 元のastro_result側にある五行
    if not any(five.values()) and isinstance(raw, dict):
        five = _deep_find_five_elements(raw)

    # 3) 最後の保険：四柱の干支から簡易算出
    if not any(five.values()):
        five = _derive_five_elements_from_pillars(shichu)

    if not any(five.values()):
        return "<p style='text-align:center;color:var(--text-dim);font-size:13px;'>五行バランスは取得できませんでした。</p>"

    colors = {"木": "#4A7C40", "火": "#C84040", "土": "#B8A060", "金": "#B0B0B0", "水": "#4060C0"}
    max_count = max([int(v or 0) for v in five.values()] + [1])

    rows = ["<div class='element-bars' style='max-width:560px;margin:16px auto 2.5rem;display:flex;flex-direction:column;gap:10px;'>"]
    for el in ["木", "火", "土", "金", "水"]:
        count = int(five.get(el) or 0)
        width = max(4 if count > 0 else 0, min(100, int(count / max_count * 100)))
        color = colors.get(el, "#999")
        rows.append(
            "<div class='element-row' style='display:flex;align-items:center;gap:10px;'>"
            f"<span class='element-name' style='width:32px;text-align:right;font-size:13px;color:{color};'>{el}</span>"
            "<div class='element-bar-bg' style='flex:1;height:6px;background:rgba(255,255,255,0.08);border-radius:999px;overflow:hidden;'>"
            f"<div class='element-bar-fill' style='width:{width}%;height:100%;background:{color};border-radius:999px;'></div>"
            "</div>"
            f"<span class='element-count' style='width:28px;font-size:12px;color:var(--text-dim);'>{count}</span>"
            "</div>"
        )
    rows.append("</div>")
    return "".join(rows)


def chapter_specs(plan: str, options: dict[str, bool] | None = None) -> list[dict[str, str]]:
    options = options or {}
    if plan == "premium":
        return [{"id": f"ch{num}", "num": num, "title": title} for num, title in CHAPTER_TITLES_PREMIUM]
    base_count = 4 if plan == "light" else 7
    base = [{"id": f"ch{num}", "num": num, "title": title} for num, title in CHAPTER_TITLES_W_SHICHU[:base_count]]
    if plan in {"light", "standard"}:
        ex_index = 1
        for key, title in OPTION_EX_TITLES.items():
            if options.get(key):
                # Avoid duplicating year forecast when standard already has ch07 enabled.
                if plan == "standard" and key == "option_year_forecast":
                    continue
                base.append({"id": f"ex{ex_index:02d}", "num": f"EX{ex_index:02d}", "title": title})
                ex_index += 1
    return base


def build_chapter_json_prompt(order: Any, *, plan: str, handoff_yaml: str, report_options: dict[str, bool]) -> str:
    specs = chapter_specs(plan, report_options)
    specs_text = "\n".join([f"- {s['num']} {s['title']}" for s in specs])
    return f"""
あなたは星月七海の鑑定書ライターです。
以下のYAMLデータを根拠に、鑑定書本文だけをJSONで生成してください。
HTML全体・CSS・ホロスコープ図・命式表は出力しません。

【出力形式】
必ずJSONのみ。Markdownコードフェンス禁止。
{{
  "cover_tagline": "表紙に入れる短い比喩コピー",
  "chapters": [
    {{
      "num": "01",
      "title": "基本性格・本質",
      "naming": "〇〇さんを一言で表すなら...です",
      "body_html": "<p>本文...</p><div class=\"callout\"><p>重要な気づき...</p></div><details class=\"evidence-block\"><summary>▼ 星の根拠</summary>...</details>",
      "closer": "章末の決め台詞"
    }}
  ]
}}

【文体ルール】
- クライアント名は「{order.customer_name}さん」で統一
- 「この人」は禁止
- 助詞直後の読点を避ける
- 1文は短め
- 章ごとにnamingとcloserを必ず入れる
- body_htmlは <p>, <strong>, <div class=\"callout\"><p>...</p></div>, <details class=\"evidence-block\"><summary>▼ 星の根拠</summary>...</details> だけを使う
- <script>, <style>, <html>, <body> は出力禁止

【今回出力する章】
{specs_text}

【プラン】
{plan}

【詳細オプション】
{json.dumps(report_options, ensure_ascii=False)}

【相談内容】
{order.consultation_text or '特記事項なし'}

【YAMLデータ】
{handoff_yaml}
""".strip()


def parse_chapter_json(text: str, specs: list[dict[str, str]]) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        data = json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, flags=re.S)
        if m:
            data = json.loads(m.group(0))
        else:
            data = {}
    chapters = data.get("chapters") if isinstance(data, dict) else None
    if not isinstance(chapters, list):
        chapters = []
    by_num = {str(c.get("num") or "").upper(): c for c in chapters if isinstance(c, dict)}
    normalized = []
    for spec in specs:
        key = str(spec["num"]).upper()
        c = by_num.get(key) or {}
        normalized.append({
            "num": spec["num"],
            "title": c.get("title") or spec["title"],
            "naming": c.get("naming") or "",
            "body_html": c.get("body_html") or "<p>本文生成に失敗しました。再生成してください。</p>",
            "closer": c.get("closer") or "",
        })
    return {"cover_tagline": (data.get("cover_tagline") if isinstance(data, dict) else "") or "星の配置から、人生の設計図を読む", "chapters": normalized}


def _cover(order: Any, raw: dict[str, Any], shichu: dict[str, Any], tagline: str, premium: bool) -> str:
    name = _safe(getattr(order, "customer_name", ""))
    title = "統合鑑定書" if premium else "総合鑑定書"
    ornament = "Western Astrology × Four Pillars × Vedic Jyotish" if premium else "Integrated Astrology × Four Pillars of Destiny"
    sub = "西洋占星術 × 四柱推命 × インド占星術　三システム統合リーディング" if premium else "ホロスコープ × 四柱推命　統合リーディング"
    if premium:
        return f"""
<div class="cover">
  <div class="cover-ornament">{ornament}</div>
  <div style="text-align:center;margin-bottom:3rem;">
    <div class="cover-name">{name}　様</div>
    <div class="cover-title">{title}</div>
    <div class="cover-sub">{sub}</div>
  </div>
  <div class="cover-divider"></div>
  <div style="text-align:center;"><div class="cover-info-table">
    <span class="cit-label">生年月日</span><span class="cit-val">{_safe(_jp_date(getattr(order,'birth_date',None)))}</span>
    <span class="cit-label">出生時刻</span><span class="cit-val">{_birth_time_place(order)}</span>
    <span class="cit-label">太陽星座</span><span class="cit-val">{_safe(_planet_label(raw,'Sun'))}</span>
    <span class="cit-label">ASC（西洋）</span><span class="cit-val">{_safe(_asc_label(raw))}</span>
    <span class="cit-label">日干（四柱）</span><span class="cit-val">{_safe(shichu.get('day_master') or '-')}</span>
    <span class="cit-label">四柱</span><span class="cit-val">{_safe(_shichu_pillars_text(shichu))}</span>
    <span class="cit-label">鑑定日</span><span class="cit-val">{_safe(_jp_date(date.today()))}</span>
  </div></div>
</div>
"""
    return f"""
<section class="cover" id="top">
  <div class="cover-ornament">{ornament}</div>
  <div class="cover-title-wrap">
    <div class="cover-name">{name} 様</div>
    <div class="cover-title-jp">{title}</div>
    <div class="cover-title-sub">{sub}</div>
  </div>
  <div class="cover-divider"></div>
  <div class="cover-info"><div class="cover-info-table">
    <span class="cover-label">生年月日</span><span class="cover-value">{_safe(_jp_date(getattr(order,'birth_date',None)))}</span>
    <span class="cover-label">出生時刻</span><span class="cover-value">{_birth_time_place(order)}</span>
    <span class="cover-label">太陽星座</span><span class="cover-value">{_safe(_planet_label(raw,'Sun'))}</span>
    <span class="cover-label">上昇点</span><span class="cover-value">{_safe(_asc_label(raw))}</span>
    <span class="cover-label">日干</span><span class="cover-value">{_safe(shichu.get('day_master') or '-')}</span>
    <span class="cover-label">四柱</span><span class="cover-value">{_safe(_shichu_pillars_text(shichu))}</span>
    <span class="cover-label">鑑定日</span><span class="cover-value">{_safe(_jp_date(date.today()))}</span>
  </div><div class="cover-tagline">\"{_safe(tagline)}\"</div></div>
</section>
"""


def _toc(specs: list[dict[str, str]], premium: bool) -> str:
    if premium:
        items = ["<li><a href=\"#charts\"><span class=\"toc-num\">図</span><span class=\"toc-chapter-title\">ホロスコープ図・命式一覧</span></a></li>"]
        for s in specs:
            items.append(f"<li><a href=\"#{s['id']}\"><span class=\"toc-num\">{_safe(s['num'])}</span><span class=\"toc-chapter-title\">{_safe(s['title'])}</span></a></li>")
        return f"<nav class='toc'><div class='toc-inner'><div class='toc-heading'>目次 — Contents</div><ul class='toc-list'>{''.join(items)}</ul></div></nav>"
    items = ["<li class=\"toc-item\"><a href=\"#charts\"><span class=\"toc-num\">図</span><span class=\"toc-chapter-title\">ホロスコープ図・四柱推命命式</span><span class=\"toc-pages\"></span></a></li>"]
    for s in specs:
        items.append(f"<li class=\"toc-item\"><a href=\"#{s['id']}\"><span class=\"toc-num\">{_safe(s['num'])}</span><span class=\"toc-chapter-title\">{_safe(s['title'])}</span><span class=\"toc-pages\"></span></a></li>")
    return f"<nav class='toc' id='toc'><div class='toc-inner'><div class='toc-heading'>目次 — Contents</div><ul class='toc-list'>{''.join(items)}</ul></div></nav>"


def _charts(raw: dict[str, Any], shichu: dict[str, Any], premium: bool) -> str:
    try:
        svg = _chart_svg(raw, size=560 if not premium else 600)
    except Exception:
        svg = ""
    chart_wrap_class = "chart-svg-wrap" if premium else "chart-frame"
    section_class = "chart-section" if premium else "chapter"
    inner_class = "chart-inner" if premium else "chapter-inner-wide"
    label = "Natal Chart — Western Astrology"
    shichu_table = _render_shichu_table(shichu, premium=premium)
    five = _render_five_elements(shichu, raw)
    return f"""
<section class="{section_class}" id="charts">
  <div class="{inner_class}">
    <div class="chapter-eyebrow"><div class="chapter-num" style="font-size:32px;">図</div><div><span class="chapter-label">Charts</span><h2 class="chapter-title">ホロスコープ図・命式一覧</h2></div></div>
    <div class="chapter-divider"></div>
    <div class="chart-section-label">{label}</div>
    <div class="{chart_wrap_class}">{svg}</div>
    <div class="chart-section-label" style="margin-top:2.5rem;">Four Pillars of Destiny — 四柱推命命式</div>
    {shichu_table}
    <div class="chart-section-label" style="margin-top:2.5rem;">Five Elements — 五行バランス</div>
    {five}
  </div>
</section>
"""


def _chapter_html(ch: dict[str, Any], premium: bool) -> str:
    section_class = "section" if premium else "chapter"
    inner_class = "section-inner" if premium else "chapter-inner"
    ch_id = f"ch{str(ch.get('num')).zfill(2)}" if str(ch.get('num')).isdigit() and premium else f"ch{ch.get('num')}".replace("chEX", "ex").lower()
    num = _safe(ch.get("num"))
    title = _safe(ch.get("title"))
    naming = ch.get("naming") or ""
    body = ch.get("body_html") or ""
    closer = ch.get("closer") or ""
    closer_html = f"<div class='closer'><p>{_safe(closer)}</p></div>" if not premium else f"<div class='closer'>{_safe(closer)}</div>"
    return f"""
<section class="{section_class}" id="{ch_id}">
  <div class="{inner_class}">
    <div class="chapter-eyebrow"><div class="chapter-num">{num}</div><div><span class="chapter-label">Chapter</span><h2 class="chapter-title">{title}</h2></div></div>
    <div class="chapter-divider"></div>
    {f'<div class="naming">{_safe(naming)}</div>' if naming else ''}
    <div class="chapter-body">{body}</div>
    {closer_html if closer else ''}
  </div>
</section>
"""


def render_external_report_html(order: Any, *, plan: str, astro_result: dict[str, Any], chapter_content: dict[str, Any], report_options: dict[str, bool]) -> str:
    premium = plan == "premium"
    css_file = "sample_kanteisho_premium_template.html" if premium else "report_black_w_shichu_template.html"
    css = _template_css(css_file)
    specs = chapter_specs(plan, report_options)
    shichu = _shichu_data(astro_result)
    chapters = chapter_content.get("chapters") or []
    # IDs should match rendered toc IDs.
    for idx, c in enumerate(chapters):
        if idx < len(specs):
            c["num"] = specs[idx]["num"]
            c["title"] = c.get("title") or specs[idx]["title"]
    body = [
        _cover(order, astro_result, shichu, chapter_content.get("cover_tagline") or "星の配置から、人生の設計図を読む", premium),
        _toc(specs, premium),
        _charts(astro_result, shichu, premium),
    ]
    body.extend(_chapter_html(c, premium) for c in chapters)
    body.append("<footer><div class='footer-logo'>星月七海 · nanami-astro</div><div class='footer-note'>この鑑定書はSwiss Ephemerisによる天体計算と、四柱推命ロジックをもとに作成しています。<br>星の流れはひとつの地図です。最終的な判断はご自身を大切にしながら行ってください。</div></footer>")
    title = "統合鑑定書" if premium else "総合鑑定書"
    return f"<!DOCTYPE html><html lang='ja'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>{_safe(title)} — {_safe(getattr(order,'customer_name',''))}様</title><link rel='preconnect' href='https://fonts.googleapis.com'><link href='https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,500;1,300;1,400&family=Noto+Serif+JP:wght@200;300;400;500&display=swap' rel='stylesheet'><style>{css}</style></head><body>{''.join(body)}</body></html>"
