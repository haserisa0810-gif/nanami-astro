import yaml

from services.handoff_log import build_handoff
from services.vedic_calc import analyze_vedic_yogas, calc_vedic_from_payload


def _planet(name, house, strength=0.6):
    return {
        "name": name,
        "house_no": house,
        "rashi_name": "Aries",
        "dignity": {"strength_score": strength},
    }


def _house_lords(mapping):
    return {str(h): {"house": h, "lord": lord} for h, lord in mapping.items()}


def test_analyze_vedic_yogas_groups_major_yogas_without_false_items():
    planets = {
        "Sun": _planet("Sun", 11),
        "Moon": _planet("Moon", 1),
        "Mars": _planet("Mars", 1),
        "Mercury": _planet("Mercury", 10),
        "Jupiter": _planet("Jupiter", 4, 0.75),
        "Venus": _planet("Venus", 9, 0.7),
        "Saturn": _planet("Saturn", 6),
        "Rahu": _planet("Rahu", 3),
        "Ketu": _planet("Ketu", 9),
    }
    house_lords = _house_lords({
        1: "Mars",
        2: "Venus",
        3: "Mercury",
        4: "Moon",
        5: "Sun",
        6: "Saturn",
        7: "Venus",
        8: "Mars",
        9: "Jupiter",
        10: "Saturn",
        11: "Saturn",
        12: "Jupiter",
    })

    result = analyze_vedic_yogas(planets, house_lords)

    assert {item["slug"] for item in result["wealth"]} >= {"dhana_yoga", "lakshmi_yoga"}
    assert {item["slug"] for item in result["career"]} >= {"raja_yoga", "viparita_raja_yoga", "amala_yoga"}
    assert {item["slug"] for item in result["mind_support"]} >= {"gajakesari_yoga", "chandra_mangala_yoga"}
    assert all(item["present"] is True for category in ("wealth", "career", "mind_support", "challenge") for item in result[category])
    assert "wealth" in result["summary"]


def test_calc_vedic_and_handoff_yaml_include_vedic_yogas():
    payload = {
        "year": 1990,
        "month": 1,
        "day": 1,
        "hour": 12,
        "minute": 0,
        "lat": 35.6895,
        "lng": 139.6917,
        "city": "Tokyo",
    }
    vedic = calc_vedic_from_payload(payload)
    handoff = build_handoff(
        inputs_view={"name": "りさテスト"},
        payload_view=payload,
        unknowns=[],
        structure_summary={"vedic": vedic},
        reports={"web": "", "raw": "", "reader": "", "line": ""},
        mode="mini",
    )
    dumped = yaml.safe_dump(handoff, allow_unicode=True, sort_keys=False)
    loaded = yaml.safe_load(dumped)

    assert "vedic_yogas" in vedic
    assert set(loaded["vedic_yogas"]) == {"summary", "wealth", "career", "mind_support", "challenge"}
    assert loaded["vedic_yogas"]["summary"]["wealth"]
    assert "YAML上に記載がないものの" not in dumped
