import json
from types import SimpleNamespace

from services.external_report_template_renderer import (
    _planet_label,
    build_chapter_json_prompt,
    parse_chapter_json,
)


PROHIBITED = [
    "処女座",
    "enemy sign",
    "enemy_sign",
    "strength_score",
    "confidence",
    "severity",
    "risk_flag",
    "active_themes",
    "overwork_health_debt",
    "obsession_burnout_cycle",
]


def test_parse_chapter_json_sanitizes_public_internal_terms():
    body = (
        "<p>金星はVirgo第6ハウスで、enemy signかつstrength_scoreは0.25。</p>"
        "<p>risk_flag overwork_health_debt severity 4 active_themes: learning_growth / inner_pressure confidence high。</p>"
        "<details class=\"evidence-block\"><summary>▼ 星の根拠</summary>"
        "<p>【西洋】金星は処女座第6ハウス</p>"
        "<p>【インド】Dhana Yoga strength: 0.8 confidence high</p>"
        "<p>【四柱推命】obsession_burnout_cycle</p></details>"
    )
    payload = {
        "cover_tagline": "Virgoとactive_themes",
        "chapters": [
            {
                "num": "01",
                "title": "Virgo",
                "naming": "enemy_sign",
                "body_html": body,
                "closer": "confidence",
            }
        ],
    }

    parsed = parse_chapter_json(json.dumps(payload, ensure_ascii=False), [{"num": "01", "title": "基本性格"}])
    rendered_text = json.dumps(parsed, ensure_ascii=False)

    assert "乙女座" in rendered_text
    for term in PROHIBITED:
        assert term not in rendered_text


def test_prompt_contains_public_evidence_and_internal_key_rules():
    order = SimpleNamespace(customer_name="りさ", consultation_text="仕事と豊かさ")
    prompt = build_chapter_json_prompt(
        order,
        plan="premium",
        handoff_yaml="vedic_yogas:\n  wealth: []\n",
        report_options={},
    )

    assert "Virgo は必ず「乙女座」" in prompt
    assert "snake_case の内部キーを本文・根拠欄に出さない" in prompt
    assert "各区分は2〜4点程度" in prompt
    assert "ヨーガ名は vedic_yogas に存在するものだけ使う" in prompt


def test_planet_label_translates_english_signs_for_cover():
    raw = {"western": {"planets": [{"name": "Sun", "sign": "Virgo", "house": 6, "deg_in_sign": 12}]}}

    assert _planet_label(raw, "Sun") == "乙女座 12度・6ハウス"
