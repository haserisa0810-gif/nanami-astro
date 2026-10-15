from __future__ import annotations

import yaml

from fixture_zodiac_2026_10 import TRANSITS
from services.note_article_service import build_note_article_yaml, extract_monthly_transit_context
from services.zodiac_context import build_zodiac_context


def _houses(ctx, key, label):
    return [x["houses"] for x in ctx[key]["aspects_by_house"] if x["label"].startswith(label)][0]


def _check_october_2026(c, *, with_motion=True):
    assert _houses(c, "Taurus", "太陽 × 金星") == [7, 7]
    assert _houses(c, "Scorpio", "太陽 × 金星") == [1, 1]
    assert _houses(c, "Leo", "金星 × 冥王星") == [4, 7]
    assert _houses(c, "Aquarius", "金星 × 冥王星") == [10, 1]
    assert _houses(c, "Aries", "火星 × 土星") == [5, 1]
    assert c["Leo"]["planets_in_sign"] == ["Jupiter", "Mars"]
    assert c["Aquarius"]["planets_in_sign"] == ["Pluto"]
    assert c["Taurus"]["moon_in_sign_dates"] == ["2026-10-26", "2026-10-27"]
    if with_motion:
        st = {(s["planet"], s["turn"]): s["date"] for s in c["Aries"]["stations_by_house"]}
        assert st[("Venus", "retrograde")] == "2026-10-03", st
        assert st[("Mercury", "retrograde")] == "2026-10-24", st
        assert c["Taurus"]["ruler"]["month_end"] == {"sign": "Lib", "house": 6, "retrograde": True}
        assert c["Virgo"]["ruler"]["month_end"]["retrograde"] is True
        assert c["Aquarius"]["placements_month_end"]["Pluto"]["retrograde"] is False
        assert c["Aquarius"]["placements_month_start"]["Pluto"]["retrograde"] is True
        assert c["Aries"]["placements_month_end"]["Saturn"]["retrograde"] is True
    assert not any(x.startswith("月") for k in c for x in c[k]["angular_aspects"])
    ing = {(g["planet"], g["date"]) for g in c["Aries"]["ingresses_by_house"]}
    assert ("Sun", "2026-10-24") in ing and ("Venus", "2026-10-26") in ing


def test_zodiac_context_october_2026_fixture():
    context = build_zodiac_context(TRANSITS)
    _check_october_2026(context, with_motion=False)
    # Legacy data contain only rounded degrees: preserve positional information,
    # but do not make up a direction or station date.
    assert context["Taurus"]["ruler"]["month_end"]["retrograde"] is None
    assert context["Aries"]["stations_by_house"] == []


def test_zodiac_context_empty_without_planet_positions():
    assert build_zodiac_context({"major_aspects": [], "daily_snapshots": [{"date": "2026-10-01"}]}) == {}


def test_zodiac_context_october_2026_real_data_in_yaml():
    context = extract_monthly_transit_context("2026-10")
    _check_october_2026(context["zodiac_context"])

    loaded = yaml.safe_load(build_note_article_yaml({"target_month": "2026-10", "transit_context": context}))
    transits = loaded["astrology_data"]["western_astrology"]["transits"]
    assert list(transits)[-1] == "zodiac_context"
    assert transits["zodiac_context"]["Taurus"]["sign_ja"] == "牡牛座"
    assert "zodiac_context" in loaded["writing_instruction"]["zodiac_writing_rule"]
