import json
from pathlib import Path

from services.western_core import WESTERN_CORE_VERSION, calc_aspects


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "western_core_golden.json"


def test_western_core_golden_cases():
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    assert fixture["version"] == WESTERN_CORE_VERSION
    for case in fixture["cases"]:
        actual = calc_aspects(case["planets"])
        expected = case["expected"]
        assert len(actual) == len(expected), case["name"]
        for actual_item, expected_item in zip(actual, expected):
            for field, value in expected_item.items():
                assert actual_item[field] == value, (
                    f"{case['name']}: {field} "
                    f"expected {value!r}, got {actual_item[field]!r}"
                )
