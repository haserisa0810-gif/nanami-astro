from __future__ import annotations

import html
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from services.result_builder import _chart_svg, _vedic_chart_svg  # existing static SVG renderers
from services.shichu_formatter import extract_shichu_data

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"

SIGN_JA = {
    "Aries": "牡羊座",
    "Taurus": "牡牛座",
    "Gemini": "双子座",
    "Cancer": "蟹座",
    "Leo": "獅子座",
    "Virgo": "乙女座",
    "Libra": "天秤座",
    "Scorpio": "蠍座",
    "Sagittarius": "射手座",
    "Capricorn": "山羊座",
    "Aquarius": "水瓶座",
    "Pisces": "魚座",
    "処女座": "乙女座",
}

SIGN_JA_SHORT = {
    "牡羊": "牡羊座",
    "牡牛": "牡牛座",
    "双子": "双子座",
    "蟹": "蟹座",
    "獅子": "獅子座",
    "乙女": "乙女座",
    "天秤": "天秤座",
    "蠍": "蠍座",
    "射手": "射手座",
    "山羊": "山羊座",
    "水瓶": "水瓶座",
    "魚": "魚座",
}

INTERNAL_PUBLIC_REPLACEMENTS = [
    (r"enemy[_ ]sign", "本来の力を少し発揮しにくい配置"),
    (r"friendly[_ ]sign", "力を自然に発揮しやすい配置"),
    (r"strength_score[^\s。<、,]*\s*[0-9.]+", "力の出方に工夫が必要"),
    (r"strength_score\s*[:=]?\s*[0-9.]+", "力の出方に工夫が必要"),
    (r"strength\s*:\s*[0-9.]+", "強さの目安が確認できる"),
    (r"strength\s+[0-9.]+", "強さの目安が確認できる"),
    (r"strength\s*:", "強さの目安は"),
    (r"confidence\s*:\s*high", "明確に確認できる"),
    (r"confidence\s+high", "明確に確認できる"),
    (r"confidence\s*:\s*medium", "中程度に確認できる"),
    (r"confidence\s+medium", "中程度に確認できる"),
    (r"confidence\s*:\s*low", "補助的に確認できる"),
    (r"confidence\s+low", "補助的に確認できる"),
    (r"confidence", "確認度"),
    (r"severity\s*[:=]?\s*4", "注意度が高い"),
    (r"severity\s*[:=]?\s*3", "注意して扱いたい"),
    (r"severity", "注意度"),
    (r"risk[_ ]flag", "注意テーマ"),
    (r"active_themes\s*:\s*learning_growth\s*/\s*inner_pressure", "学び直しと内側からのプレッシャー"),
    (r"active_themes\s*:\s*inner_pressure\s*/\s*learning_growth", "内側からのプレッシャーと学び直し"),
    (r"active_themes\s*:\s*learning_growth", "学び直しや成長がテーマ"),
    (r"active_themes\s*:\s*inner_pressure", "内側からのプレッシャーが強まりやすい"),
    (r"active_themes", "現在強まりやすいテーマ"),
    (r"learning_growth", "学び直しや成長"),
    (r"inner_pressure", "内側からのプレッシャー"),
    (r"overwork_health_debt", "頑張りすぎが体調や気力の負債になりやすい傾向"),
    (r"obsession_burnout_cycle", "集中しすぎて消耗へ向かいやすい傾向"),
    (r"raw YAML key", "内部データ名"),
]

PROHIBITED_PUBLIC_TERMS = [
    "enemy sign",
    "enemy_sign",
    "friendly_sign",
    "strength_score",
    "confidence",
    "severity",
    "risk_flag",
    "active_themes",
    "overwork_health_debt",
    "obsession_burnout_cycle",
]

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


def _external_report_layout_css() -> str:
    return """
.cover-info,
.cover-info > div,
.cover-info-table {
  width: min(100%, 680px);
  max-width: 680px;
  margin-left: auto;
  margin-right: auto;
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
  overflow-wrap: anywhere !important;
  line-height: 1.65 !important;
}
.cover-label,
.cit-label {
  min-width: 6.5em;
  white-space: nowrap !important;
  letter-spacing: .08em !important;
}
.cover-value,
.cit-val {
  min-width: 0;
}
""".strip()


def _safe(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _normalize_public_astrology_terms(text: Any) -> str:
    out = "" if text is None else str(text)
    if not out:
        return ""
    out = out.replace("処女座", "乙女座")
    for src, dst in SIGN_JA.items():
        out = re.sub(re.escape(src), dst, out, flags=re.I)
    for src, dst in SIGN_JA_SHORT.items():
        out = re.sub(rf"{re.escape(src)}(?!座)", dst, out)
    for pattern, replacement in INTERNAL_PUBLIC_REPLACEMENTS:
        out = re.sub(pattern, replacement, out, flags=re.I)
    # Last-resort cleanup for snake_case tokens that slipped into prose.
    out = re.sub(r"\b[a-z]+(?:_[a-z0-9]+)+\b", "内部テーマ", out)
    return out


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
            return _normalize_public_astrology_terms(f"{sign}{deg_s}{house_s}".strip()) or "-"
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
                return _normalize_public_astrology_terms(f"{sign} {float(deg):.0f}度".strip())
            except Exception:
                return _normalize_public_astrology_terms(sign or "-")
        if c:
            return _normalize_public_astrology_terms(c)
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
    premium_integration_rules = ""
    if plan == "premium":
        premium_integration_rules = """
【プレミアム三術統合ルール（必須）】
- プレミアム鑑定書は、西洋占星術・インド占星術・四柱推命の3占術を統合して本文を書く
- 西洋占星術だけ、または西洋占星術＋四柱推命だけで本文を作らない
- 各章で最低1回はインド占星術の要素を本文に反映する
- インド占星術は補足ではなく、章テーマを読むための根拠のひとつとして扱う
- 入力YAMLに存在しないインド占星術要素は追加しない
- ナクシャトラ、ラグナ、月、ダシャー、ハウス支配星、ヨーガなど、YAMLに存在する範囲で使う
- ナヴァムシャ、サディサティなどがYAMLに無い場合は書かない
- 「インド占星術では〜」という一般論で埋めず、必ずYAML内の計算済みデータを根拠にする
- 西洋・インド・四柱推命を無理に均等配分せず、章テーマごとに最も効く要素を選ぶ
- 西洋・インド・四柱推命を順番に独立解説しない。三占術は一つの人物像を補強するために統合する
- 三占術が同じ方向を指すときは一致点を最優先し、「この人は何で評価され、何をすると運が動くか」まで書く
- 占術用語の定義説明より、本人の矛盾・使い方・行動の癖を優先する
- ただし各章末の根拠欄には、3占術すべての根拠を必ず出す

【章ごとの三術接続】
- 基本性格・本質: 西洋は太陽・月・ASC・ハウス集中、インドはラグナ・月・ナクシャトラ、四柱推命は日干・月令・五行バランスを見る
- 才能・適性・仕事運: 西洋は仕事ハウス・MC・アスペクト、インドは10室・10室支配星・ダシャー・適職に関係する配置、四柱推命は食傷・財星・官星・五行の偏りを見る
- 恋愛・パートナーシップ: 西洋は7ハウス・金星・月・対人アスペクト、インドは7室・7室支配星・金星・ナヴァムシャ情報がYAMLにあればそれ、四柱推命は財星・官星・日支・配偶者宮を見る
- お金・豊かさ: 西洋は2ハウス・8ハウス・金星・木星、インドは2室・11室・ダナヨーガ・ダシャー、四柱推命は財星・食傷生財・五行バランスを見る
- 魂の課題・人生テーマ: 西洋はノード・土星・冥王星・キロン、インドはラーフ・ケートゥ・月・ナクシャトラ・ダシャー、四柱推命は用神的な偏り・空亡・大運の流れを見る
- 年運・トランジット: 西洋はトランジット、インドはダシャーとYAMLにある時期要素、四柱推命は大運・流年を見る

【プレミアム根拠欄の必須書式】
各章の body_html 末尾に、必ず次の形の details を入れる。
<details class=\"evidence-block\"><summary>▼ 星の根拠</summary><p>【西洋】...</p><p>【インド】...</p><p>【四柱推命】...</p></details>
- 【西洋】には、その章で使った西洋占星術の主要根拠を入れる
- 【インド】には、その章で使ったインド占星術の主要根拠を入れる
- 【四柱推命】には、その章で使った四柱推命の主要根拠を入れる
- 各区分は2〜4点程度に抑え、「・根拠 + 短い補足」の形にする
- 根拠欄に enemy_sign / strength_score / confidence / severity / risk_flag / active_themes などの内部キーを出さない
- 根拠欄に英語星座名や「処女座」を出さず、必ず日本語星座名で書く
- インドのヨーガは vedic_yogas に存在するものだけを書く。strength / confidence の数値は書かない
- 該当章で使えるインド占星術データがYAML上限定的な場合は、捏造せず「【インド】入力データ上、この章で直接使える追加根拠は限定的」と書く
- 根拠欄を【西洋】と【命式】だけで終わらせない
""".strip()
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

【日本語表記ルール】
- 星座名は本文・根拠欄とも日本語表記に統一する
- Virgo は必ず「乙女座」と書く。「処女座」は使用禁止
- Cancer は「蟹座」、Leo は「獅子座」、Aquarius は「水瓶座」と書く
- Aries=牡羊座、Taurus=牡牛座、Gemini=双子座、Libra=天秤座、Scorpio=蠍座、Sagittarius=射手座、Capricorn=山羊座、Pisces=魚座
- インド占星術のRashi名も、本文では日本語星座名を優先する

【内部値を本文・根拠欄に出さない】
- 次の語をそのまま出力禁止: enemy_sign, enemy sign, friendly_sign, strength_score, strength:, confidence:, confidence, severity, risk_flag, active_themes, overwork_health_debt, obsession_burnout_cycle, raw YAML key
- snake_case の内部キーを本文・根拠欄に出さない
- YAMLのキー名やデバッグ値を引用せず、必ず読み手向けの自然文に直す
- enemy sign は「本来の力を少し発揮しにくい配置」
- strength_score が低い場合は「力の出方に工夫が必要」
- confidence high は「明確に確認できる」
- severity 4 は「注意度が高い」
- overwork_health_debt は「頑張りすぎが体調や気力の負債になりやすい」
- obsession_burnout_cycle は「集中しすぎて消耗へ向かいやすい」
- active_themes: learning_growth は「学び直しや成長がテーマ」
- active_themes: inner_pressure は「内側からのプレッシャーが強まりやすい」

【占術知識ではなく、その人を書く】
- 鑑定書の主役は占術知識ではなく、{order.customer_name}さん本人
- 「ダニシュタとは〜」「Raja Yogaとは〜」「第6ハウスとは〜」のような百科事典的説明で段落を始めない
- 占術用語は「{order.customer_name}さんにはどう現れるか」「強み・課題・行動傾向として何を意味するか」を書くためだけに使う
- ナクシャトラやヨーガを説明するときも、定義ではなく本人の動き方へ直結させる
- 悪い例: ダニシュタは豊かさと音楽のナクシャトラです
- 良い例: 月がダニシュタ第3パダにあるため、考え続けるより小さく作って出すほど感情が整理される
- 悪い例: Raja Yogaは社会的評価を示すヨーガです
- 良い例: Raja Yogaが確認できるため、日々の実務を磨くほど信頼が積み上がりやすい

【三占術を独立解説しない】
- 「西洋では〜。インドでは〜。四柱推命では〜。まとめると〜」という順番の並列説明は禁止
- まず章の主役となる人物像を一つ決める
- 西洋占星術で主軸を作り、インド占星術は裏付け・補強・修正として使う
- 四柱推命は現実面、行動パターン、時期性を補強するために使う
- 三占術が同じ方向を指す場合は、その一致点から「この人だからこその結論」を作る
- 同じテーマを三回説明せず、人物像を一つ深めるために統合する
- 最後は必ず「だから何をすると運が動くのか」へ着地する

【驚きのある個人鑑定にする】
- 一般論だけで終わらせず、「これは自分のことだ」と感じる一文を各章に入れる
- その人固有の矛盾、一見反対に見える配置の共存、三占術が同じ方向を指す意外な一致を優先する
- 一般的な長所ではなく、その人特有の使い方を書く
- 内部値から読める意味は削除して終わらせず、人物像へ変換する
- 例: 本来の力を少し発揮しにくい配置 → 才能はあるが、自然に出るより経験を通じて育つ
- 例: 学び直しや成長がテーマ → 今は知識を増やすだけでなく、現実の行動へ移す時期

【各章に驚きポイントを必ず入れる】
- 各章の本文に最低1つ、{order.customer_name}さん固有の矛盾・癖・盲点・行動パターンを刺す一文を入れる
- これは過激な断定や不安を煽る文章ではなく、配置の組み合わせから出る「言われると痛いけど当たっている」観察にする
- 悪い例: あなたは努力家です
- 悪い例: 自分らしさを大切にしましょう
- 悪い例: 無理せず休むことが大切です
- 良い例: 技術不足で止まる人ではありません。むしろ「まだ十分ではない」と判断して、自分で出すタイミングを遅らせやすい人です
- 良い例: 稼ぐ力が弱いのではなく、自分の貢献を安く見積もることで、お金の流れを自分で細くしやすい配置です
- 良い例: 人前に出たいわけではないのに、自分で仕組みを作ると、結局その仕組みの中心人物になりやすい人です
- 良い例: 疲れている時ほど休むのではなく、さらに整えようとしてしまう傾向があります

【優等生すぎる助言で終わらせない】
- 「自分らしさを大切にしてください」「無理せず休みましょう」「焦らず進みましょう」「技術を磨きましょう」「人とのつながりを大切にしましょう」だけで段落や章を終えない
- これらを書く場合は、必ず{order.customer_name}さん固有の癖や矛盾に接続する
- 悪い例: 無理せず休みましょう
- 良い例: {order.customer_name}さんの場合、「疲れたから休む」では遅くなりやすいです。まだ動ける日のうちに休みを予定へ入れることが、長く燃え続けるための管理になります

【三占術の一致から刺さる一文を作る】
- 驚きポイントは、なるべく三占術の一致点から作る
- 例: 西洋の第6ハウス集中、インドのRaja Yoga、四柱推命の食神生財がそろう場合、「技術を磨くほど評価される」で止めない
- その場合は「ただし、完成度を求めすぎるほど公開が遅れ、評価される前に自分で止めてしまいやすい」のように盲点まで書く

【章ごとの驚きポイントの方向性】
- 基本性格: 情熱的なのに勢いだけでは動けない / 直感はあるが根拠が揃わないと自分を信じきれない / 目立ちたいのではなく正確に役に立つことで存在感が出る
- 仕事: 実力不足ではなく出すタイミングの遅さが課題 / 裏方で仕組みを作るほど中心人物になりやすい / 苦労や理不尽な経験が後から信用の証拠になる
- 恋愛: 深く関わりたいのに頼る前に自分で処理してしまいやすい / 愛情表現が「好き」より「整える」「手伝う」「改善する」に出やすい / 近づきたいが近づきすぎると空間を守りたくなる
- お金: 稼ぐ力より値付け・受け取り・自己評価が課題になりやすい / 自分の仕事を「これくらい」と安く見積もりやすい / 貢献量と受け取り量のバランスを意識的に作る必要がある
- 健康: 疲れたら休むでは遅くなりやすい / まだ動ける日に休みを入れる必要がある / 整えようとしてさらに消耗することがある
- 魂の課題: 一人でできる力が強すぎて深い関係に入る前に自己完結しやすい / 完璧に準備してから出そうとして魂が求める表現を遅らせやすい / 怖い方向にこそ次の扉がある

【驚きポイントの書き方】
- 読者を責めない。決めつけすぎない。不安を煽らない
- ただし丸めすぎない。「良い面も悪い面もあります」で終わらせない
- 矛盾を矛盾のまま残し、「だからどう使うか」まで書く
- 占術用語より、読者の行動パターンが印象に残るようにする
- 驚きポイントは本文に入れる。根拠欄には書きすぎない

【一言キャッチコピーの扱い】
- 第1章で決めた「この人の核」を、以後の章でも変奏する
- 各章の naming は章ごとに別人化させない
- 悪い例: 第1章は精密な炎、第2章は技術者、第3章は自由な恋人、のように核が消える
- 良い例: 第1章で「精密な炎」と決めたなら、仕事章は「精密な炎が仕事で形になる人」、恋愛章は「精密な炎を深い関係の中で調整する人」

【根拠欄の方針】
- 根拠欄は残すが、読み手向けの「証拠一覧＋短い補足」にする
- 各区分は2〜4点程度に抑える。長文解説は本文側に回す
- 根拠欄ではデバッグ値、英語キー、snake_case、数値スコアを出さない
- 書式は <details class=\"evidence-block\"><summary>▼ 星の根拠</summary><p>【西洋】・...<br>・...</p><p>【インド】・...<br>・...</p><p>【四柱推命】・...<br>・...</p></details>
- 【西洋】例: ・第6ハウスに水星・金星・火星が集中<br>・太陽は獅子座第5ハウス
- 【インド】例: ・魚座ラグナ：直感を現実の役割へ落とし込みやすい<br>・月はダニシュタ第3パダ：感情を行動へ変えやすい<br>・Raja Yoga：努力や役割が評価につながりやすい
- 【四柱推命】例: ・日干は甲<br>・五行は火が強く、水が表に出ていない

【ヨーガ表記】
- ヨーガ名は vedic_yogas に存在するものだけ使う
- strength / confidence は本文・根拠欄に数値やキー名として出さない
- 必要なら「明確に成立」「中程度に確認」「補助的に確認」など自然文で表現する
- 「YAML上に記載がないものの」は禁止
- 成立していないヨーガを別のヨーガで代用しない

【主役を決めてから書く（最優先）】
- 書き始める前に、この鑑定書の主役を最大2つ特定する
- 主役の選定は、同一ハウスの天体集中（3天体以上）、アングル（1・4・7・10ハウス）上の天体、太陽・月・ASC、オーブの小さいタイトなアスペクトの順で優先する
- 主役は最大2つ。3つ以上を同列に並べない
- 各章は主役の軸から構成し、主役と無関係な配置は均等に並べず軽く触れるに留める
- アスペクトは単独解説にせず、主役の説明を深める根拠として従属させる
- 矛盾する配置は「結果的にバランスが取れている」とまとめず、両方を同時に存在するものとして残す
- トランジットは「主役 → 今どこが刺激されているか → だから何が起きやすいか」の順で読む

{premium_integration_rules}

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
            "title": _normalize_public_astrology_terms(c.get("title") or spec["title"]),
            "naming": _normalize_public_astrology_terms(c.get("naming") or ""),
            "body_html": _normalize_public_astrology_terms(c.get("body_html") or "<p>本文生成に失敗しました。再生成してください。</p>"),
            "closer": _normalize_public_astrology_terms(c.get("closer") or ""),
        })
    tagline = (data.get("cover_tagline") if isinstance(data, dict) else "") or "星の配置から、人生の設計図を読む"
    return {"cover_tagline": _normalize_public_astrology_terms(tagline), "chapters": normalized}


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
    try:
        vedic_svg = _vedic_chart_svg(raw, size=560) if premium else ""
    except Exception:
        vedic_svg = ""
    chart_wrap_class = "chart-svg-wrap" if premium else "chart-frame"
    section_class = "chart-section" if premium else "chapter"
    inner_class = "chart-inner" if premium else "chapter-inner-wide"
    label = "Natal Chart — Western Astrology"
    shichu_table = _render_shichu_table(shichu, premium=premium)
    five = _render_five_elements(shichu, raw)
    vedic_page = (
        f"""
    <div class="chart-page chart-page-vedic">
      <div class="chart-section-label">Vedic Astrology — Rashi Chart</div>
      <div class="{chart_wrap_class} vedic-chart-frame">{vedic_svg}</div>
    </div>
"""
        if vedic_svg
        else ""
    )
    return f"""
<section class="{section_class}" id="charts">
  <div class="{inner_class}">
    <div class="chapter-eyebrow"><div class="chapter-num" style="font-size:32px;">図</div><div><span class="chapter-label">Charts</span><h2 class="chapter-title">ホロスコープ図・命式一覧</h2></div></div>
    <div class="chapter-divider"></div>
    <div class="chart-page chart-page-western">
      <div class="chart-section-label">{label}</div>
      <div class="{chart_wrap_class}">{svg}</div>
    </div>
    <div class="chart-page chart-page-shichu">
      <div class="chart-section-label">Four Pillars of Destiny — 四柱推命命式</div>
      {shichu_table}
      <div class="chart-section-label chart-section-label-five">Five Elements — 五行バランス</div>
      {five}
    </div>
{vedic_page}
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
<section class="{section_class} report-body-page" id="{ch_id}">
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
    css = f"{_template_css(css_file)}\n\n{_external_report_layout_css()}"
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
    footer_basis = (
        "この鑑定書は、Swiss Ephemerisによる西洋占星術の天体計算、インド占星術ロジック、四柱推命ロジックをもとに作成しています。"
        if premium
        else "この鑑定書はSwiss Ephemerisによる天体計算と、四柱推命ロジックをもとに作成しています。"
    )
    body.append(f"<footer><div class='footer-logo'>星月七海 · nanami-astro</div><div class='footer-note'>{footer_basis}<br>星の流れはひとつの地図です。最終的な判断はご自身を大切にしながら行ってください。</div></footer>")
    title = "統合鑑定書" if premium else "総合鑑定書"
    return f"<!DOCTYPE html><html lang='ja'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'><title>{_safe(title)} — {_safe(getattr(order,'customer_name',''))}様</title><link rel='preconnect' href='https://fonts.googleapis.com'><link href='https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,500;1,300;1,400&family=Noto+Serif+JP:wght@200;300;400;500&display=swap' rel='stylesheet'><style>{css}</style></head><body>{''.join(body)}</body></html>"
