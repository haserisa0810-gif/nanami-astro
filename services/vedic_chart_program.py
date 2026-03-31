from __future__ import annotations

import argparse
import html
import sys
from pathlib import Path
from typing import Any, Dict, List

# 使い方想定:
# 1) このファイルを nanami-astro/ 配下に置く
# 2) python vedic_chart_program.py --year 1990 --month 1 --day 1 --hour 12 --minute 0 --lat 35.68 --lng 139.76
# 3) output/vedic_chart.svg が生成される

# 既存の services/vedic_calc.py を使う前提
ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT / "nanami-astro" if (ROOT / "nanami-astro").exists() else ROOT
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from services.vedic_calc import calc_vedic_from_payload, RASHI_NAMES
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "services.vedic_calc を読み込めませんでした。\n"
        "このファイルを nanami-astro/ 直下に置いて実行してください。\n"
        f"詳細: {e}"
    )

SIGN_ABBR = {
    "Aries": "Ar", "Taurus": "Ta", "Gemini": "Ge", "Cancer": "Cn",
    "Leo": "Le", "Virgo": "Vi", "Libra": "Li", "Scorpio": "Sc",
    "Sagittarius": "Sg", "Capricorn": "Cp", "Aquarius": "Aq", "Pisces": "Pi",
}

PLANET_ABBR = {
    "Asc": "Asc",
    "Sun": "Su",
    "Moon": "Mo",
    "Mars": "Ma",
    "Mercury": "Me",
    "Jupiter": "Ju",
    "Venus": "Ve",
    "Saturn": "Sa",
    "Rahu": "Ra",
    "Ketu": "Ke",
    "Uranus": "Ur",
    "Neptune": "Ne",
    "Pluto": "Pl",
}

# South Indian chart: sign-fixed layout
# 4x4 の外周12マスを使用する
SOUTH_INDIAN_SIGN_GRID = {
    1: (0, 1),   # Aries
    2: (0, 0),   # Taurus
    3: (1, 0),   # Gemini
    4: (2, 0),   # Cancer
    5: (3, 0),   # Leo
    6: (3, 1),   # Virgo
    7: (3, 2),   # Libra
    8: (3, 3),   # Scorpio
    9: (2, 3),   # Sagittarius
    10: (1, 3),  # Capricorn
    11: (0, 3),  # Aquarius
    12: (0, 2),  # Pisces
}


def _planet_lines(result: Dict[str, Any], include_outer: bool = False) -> Dict[int, List[str]]:
    planets = result.get("planets_map") or result.get("planets") or {}
    if isinstance(planets, list):
        planets = {p.get("name"): p for p in planets if isinstance(p, dict) and p.get("name")}
    asc = result.get("ascendant") or {}
    grouped: Dict[int, List[str]] = {i: [] for i in range(1, 13)}

    if asc and asc.get("rashi_no"):
        asc_rashi_no = int(asc["rashi_no"])
        grouped[asc_rashi_no].append("Asc")

    order = ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]
    if include_outer:
        order += ["Uranus", "Neptune", "Pluto"]

    for name in order:
        pdata = planets.get(name)
        if not pdata:
            continue
        rashi_no = int(pdata["rashi_no"])
        deg = float(pdata.get("deg_in_sign", 0.0))
        grouped[rashi_no].append(f"{PLANET_ABBR.get(name, name)} {deg:04.1f}°")

    return grouped


def build_south_indian_svg(
    result: Dict[str, Any],
    title: str = "Vedic Horoscope",
    width: int = 900,
    height: int = 900,
    include_outer: bool = False,
) -> str:
    margin = 40
    board = min(width, height) - margin * 2
    cell = board / 4
    x0 = (width - board) / 2
    y0 = (height - board) / 2
    grouped = _planet_lines(result, include_outer=include_outer)

    asc = result.get("ascendant") or {}
    asc_rashi_no = int(asc["rashi_no"]) if asc and asc.get("rashi_no") else None

    parts: List[str] = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    parts.append('<style>')
    parts.append('text{font-family:Arial,"Hiragino Sans","Yu Gothic",sans-serif;fill:#222;}')
    parts.append('.title{font-size:28px;font-weight:700;}')
    parts.append('.sign{font-size:15px;font-weight:700;}')
    parts.append('.planet{font-size:16px;}')
    parts.append('.small{font-size:12px;fill:#666;}')
    parts.append('.cell{fill:#fff;stroke:#222;stroke-width:2;}')
    parts.append('.ascCell{fill:#f8f3d9;}')
    parts.append('</style>')

    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="#fafafa"/>')
    parts.append(f'<text x="{width/2}" y="30" text-anchor="middle" class="title">{html.escape(title)}</text>')

    # 4x4 グリッド
    for r in range(4):
        for c in range(4):
            x = x0 + c * cell
            y = y0 + r * cell
            # 中央2x2は空白
            if 1 <= r <= 2 and 1 <= c <= 2:
                parts.append(f'<rect x="{x}" y="{y}" width="{cell}" height="{cell}" fill="#ffffff" stroke="#222" stroke-width="2"/>')
                continue

            sign_no = None
            for k, v in SOUTH_INDIAN_SIGN_GRID.items():
                if v == (r, c):
                    sign_no = k
                    break

            classes = 'cell'
            if sign_no and asc_rashi_no == sign_no:
                classes += ' ascCell'
            parts.append(f'<rect x="{x}" y="{y}" width="{cell}" height="{cell}" class="{classes}"/>')

            if sign_no:
                sign_name = RASHI_NAMES[sign_no - 1]
                sign_label = f'{sign_no} {SIGN_ABBR.get(sign_name, sign_name)}'
                parts.append(f'<text x="{x+12}" y="{y+22}" class="sign">{html.escape(sign_label)}</text>')
                if asc_rashi_no == sign_no:
                    parts.append(f'<text x="{x+cell-12}" y="{y+22}" text-anchor="end" class="sign">Lagna</text>')

                lines = grouped.get(sign_no, [])
                line_y = y + 46
                for line in lines[:8]:
                    parts.append(f'<text x="{x+12}" y="{line_y}" class="planet">{html.escape(line)}</text>')
                    line_y += 22

    center_x = x0 + board / 2
    center_y = y0 + board / 2
    meta1 = []
    if result.get("ascendant"):
        meta1.append(f"Asc: {result['ascendant'].get('rashi_name', '-')}")
    if result.get("moon_nakshatra"):
        meta1.append(f"Nakshatra: {result['moon_nakshatra']}")
    parts.append(f'<text x="{center_x}" y="{center_y-10}" text-anchor="middle" class="title" style="font-size:22px">Rashi Chart</text>')
    parts.append(f'<text x="{center_x}" y="{center_y+20}" text-anchor="middle" class="small">{html.escape(" | ".join(meta1))}</text>')
    parts.append(f'<text x="{center_x}" y="{center_y+42}" text-anchor="middle" class="small">South Indian style / sign-fixed</text>')
    parts.append('</svg>')
    return "".join(parts)


def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    planets = result.get("planets_map") or result.get("planets") or {}
    if isinstance(planets, list):
        planets = {p.get("name"): p for p in planets if isinstance(p, dict) and p.get("name")}
    moon = planets.get("Moon") or {}
    asc = result.get("ascendant") or result.get("asc_data") or {}
    out = dict(result)
    out["ascendant"] = asc
    out["moon_nakshatra"] = moon.get("nakshatra_name")
    return out


def render_svg_from_payload(payload: Dict[str, Any], title: str = "Vedic Horoscope") -> str:
    result = calc_vedic_from_payload(payload)
    result = normalize_result(result)
    return build_south_indian_svg(result, title=title)


def main() -> None:
    parser = argparse.ArgumentParser(description="インド占星術のホロスコープ図（South Indian style SVG）を生成")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument("--hour", type=int, default=12)
    parser.add_argument("--minute", type=int, default=0)
    parser.add_argument("--lat", type=float, required=True)
    parser.add_argument("--lng", type=float, required=True)
    parser.add_argument("--name", type=str, default="Vedic Horoscope")
    parser.add_argument("--output", type=str, default="output/vedic_chart.svg")
    args = parser.parse_args()

    payload = {
        "year": args.year,
        "month": args.month,
        "day": args.day,
        "hour": args.hour,
        "minute": args.minute,
        "lat": args.lat,
        "lng": args.lng,
    }

    svg = render_svg_from_payload(payload, title=args.name)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg, encoding="utf-8")
    print(f"saved: {out}")


if __name__ == "__main__":
    main()
