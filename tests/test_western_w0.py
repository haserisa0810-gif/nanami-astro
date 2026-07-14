from services.transit_calc import _match_aspects
from services.western_calc import ASPECTS, ORB, calc_aspects, calc_western_from_payload
from shared import _calc_payload_from_inputs


def _aspect(items, aspect_type):
    return next(item for item in items if item["type"] == aspect_type)


def _legacy_aspects(planets):
    """W0追加前と同じ検出条件で後方互換の集合を作る。"""
    from services.western_calc import angle_diff

    result = []
    for index, first in enumerate(planets):
        for second in planets[index + 1:]:
            separation = angle_diff(first["lon"], second["lon"])
            for name, exact_angle in ASPECTS.items():
                orb = abs(separation - exact_angle)
                if orb <= ORB[name]:
                    result.append((first["name"], second["name"], name, round(orb, 2)))
    return result


def test_applying_formula_examples_true_false_and_unknown():
    applying = calc_aspects([
        {"name": "A", "lon": 10.0, "speed": 1.0},
        {"name": "B", "lon": 101.0, "speed": 0.0},
    ])
    separating = calc_aspects([
        {"name": "A", "lon": 10.0, "speed": 0.0},
        {"name": "B", "lon": 101.0, "speed": 1.0},
    ])
    unknown = calc_aspects([
        {"name": "A", "lon": 10.0, "speed": None},
        {"name": "B", "lon": 101.0, "speed": None},
    ])

    assert _aspect(applying, "square")["applying"] is True
    assert _aspect(separating, "square")["applying"] is False
    assert _aspect(unknown, "square")["applying"] is None


def test_signed_deviation_absolute_value_matches_existing_orb():
    items = calc_aspects([
        {"name": "A", "lon": 10.0, "speed": 1.0},
        {"name": "B", "lon": 11.5, "speed": 0.5},
        {"name": "C", "lon": 71.0, "speed": 0.2},
        {"name": "D", "lon": 101.0, "speed": 0.0},
        {"name": "E", "lon": 131.0, "speed": -0.1},
        {"name": "F", "lon": 191.0, "speed": -0.2},
    ])

    assert items
    for item in items:
        assert abs(item["signed_deviation"]) == item["orb"]


def test_applying_is_symmetric_for_negative_and_positive_signed_separation():
    negative = calc_aspects([
        {"name": "A", "lon": 10.0, "speed": 1.0},
        {"name": "B", "lon": 101.0, "speed": 0.0},
    ])
    positive = calc_aspects([
        {"name": "A", "lon": 101.0, "speed": 0.0},
        {"name": "B", "lon": 10.0, "speed": 1.0},
    ])

    assert _aspect(negative, "square")["applying"] is True
    assert _aspect(positive, "square")["applying"] is True


def test_real_western_result_contains_speed_rules_quality_and_new_aspect_fields():
    result = calc_western_from_payload({
        "year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30,
        "lat": 35.6895, "lng": 139.6917,
        "include_chiron": False, "include_lilith": False,
    })

    assert result["planets"]
    assert all("speed" in planet for planet in result["planets"])
    moving_bodies = [planet for planet in result["planets"] if planet["speed"] is not None]
    assert moving_bodies
    assert all(planet["retrograde"] is (planet["speed"] < 0) for planet in moving_bodies)
    required = {
        "planet1", "planet2", "type", "orb", "exact_angle", "actual_angle",
        "signed_deviation", "orb_limit", "applying",
    }
    assert result["aspects"]
    assert all(required.issubset(item) for item in result["aspects"])
    assert result["calculation_rules"]["rule_set_id"] == "western_default_v1"
    assert result["calculation_rules"]["house_system"] == "P"
    assert result["data_quality"] == {
        "birth_time": "known", "houses_available": True, "coordinates": "provided"
    }
    assert result["engine_version_western"] == "w0.1.0"


def test_south_node_aspects_alone_receive_axis_mirror_flag():
    items = calc_aspects([
        {"name": "X", "lon": 0.0, "speed": 1.0},
        {"name": "North Node", "lon": 180.0, "speed": -0.05},
        {"name": "South Node", "lon": 0.0, "speed": -0.05},
    ])

    south_items = [item for item in items if "South Node" in (item["planet1"], item["planet2"])]
    north_only = [item for item in items if "North Node" in (item["planet1"], item["planet2"]) and "South Node" not in (item["planet1"], item["planet2"])]
    assert south_items and all(item["axis_mirror"] is True for item in south_items)
    assert north_only and all("axis_mirror" not in item for item in north_only)


def test_explicit_noon_birth_time_is_known_via_payload_birth_time_key():
    # shared.py の payload は birth_time 原文を保持するため、真の12:00生まれは known になる
    result = calc_western_from_payload({
        "year": 1990, "month": 5, "day": 15, "hour": 12, "minute": 0,
        "birth_time": "12:00",
        "lat": 35.6895, "lng": 139.6917,
        "include_chiron": False, "include_lilith": False,
    })
    assert result["data_quality"]["birth_time"] == "known"

    # birth_time=None（未入力→12:00仮置き）は unknown に倒れる
    fallback = calc_western_from_payload({
        "year": 1990, "month": 5, "day": 15, "hour": 12, "minute": 0,
        "birth_time": None,
        "lat": 35.6895, "lng": 139.6917,
        "include_chiron": False, "include_lilith": False,
    })
    assert fallback["data_quality"]["birth_time"] == "unknown"


def test_missing_birth_time_is_reported_as_unknown_and_houses_not_reliable():
    result = calc_western_from_payload({
        "year": 1990, "month": 5, "day": 15,
        "lat": 35.6895, "lng": 139.6917,
        "include_chiron": False, "include_lilith": False,
    })

    assert result["data_quality"]["birth_time"] == "unknown"
    assert result["data_quality"]["houses_available"] is False


def test_payload_builder_preserves_unknown_and_exact_noon_birth_time():
    unknowns = []
    unknown_payload = _calc_payload_from_inputs(
        "1990-05-15", None, "東京", None, 35.6895, 139.6917, unknowns
    )
    known_payload = _calc_payload_from_inputs(
        "1990-05-15", "12:00", "東京", None, 35.6895, 139.6917, []
    )

    assert unknown_payload["birth_time"] is None
    assert calc_western_from_payload(unknown_payload)["data_quality"]["birth_time"] == "unknown"
    assert known_payload["birth_time"] == "12:00"
    assert calc_western_from_payload(known_payload)["data_quality"]["birth_time"] == "known"


def test_existing_aspect_set_orbs_and_required_keys_are_unchanged():
    result = calc_western_from_payload({
        "year": 1985, "month": 9, "day": 2, "hour": 17, "minute": 45,
        "lat": 34.6937, "lng": 135.5023,
        "include_chiron": False, "include_lilith": False,
    })
    legacy = _legacy_aspects(result["planets"])
    current = [
        (item["planet1"], item["planet2"], item["type"], item["orb"])
        for item in result["aspects"]
    ]

    assert set(current) == set(legacy)
    assert len(current) == len(legacy)
    assert all({"name", "lon", "sign", "house", "retrograde"}.issubset(planet) for planet in result["planets"])
    assert all({"planet1", "planet2", "type", "orb"}.issubset(item) for item in result["aspects"])


def test_transit_match_adds_applying_with_natal_speed_fixed_at_zero():
    hits = _match_aspects(
        [{"name": "Sun", "lon": 10.0, "speed": 1.0, "sign": "Ari"}],
        [{"name": "Moon", "lon": 101.0, "speed": 99.0, "sign": "Can"}],
    )

    square = next(item for item in hits if item["aspect"] == "square")
    assert square["applying"] is True
    assert square["exact_angle"] == 90
    assert square["signed_deviation"] == 1.0
    assert square["orb_limit"] == 2.0
