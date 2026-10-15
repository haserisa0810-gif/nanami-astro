"""Monthly station dates from unrounded tropical longitude velocity.

Daily rounded positions cannot identify stations reliably. Search the change
of velocity sign, including the interval preceding the month, in JST.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import swisseph as swe

from services.western_calc import configure_ephemeris, sign_of


JST = ZoneInfo("Asia/Tokyo")
STATION_BODIES = {"Mercury": swe.MERCURY, "Venus": swe.VENUS, "Mars": swe.MARS}


def calc_monthly_station_events(year: int, month: int, *, position_loader=None) -> list[dict]:
    """Find Mercury/Venus/Mars stations to within one second; month is [start, end)."""
    start = datetime(year, month, 1, tzinfo=JST)
    end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=JST)
    if position_loader is None:
        flags = configure_ephemeris() | swe.FLG_SPEED

        def position_loader(dt, planet):
            utc = dt.astimezone(timezone.utc)
            hour = utc.hour + utc.minute / 60 + (utc.second + utc.microsecond / 1e6) / 3600
            jd = swe.julday(utc.year, utc.month, utc.day, hour)
            values, _ = swe.calc_ut(jd, STATION_BODIES[planet], flags)
            return {"speed": float(values[3]), "sign": sign_of(values[0])[0]}

    events = []
    left = start - timedelta(days=1)
    previous = {name: position_loader(left, name) for name in STATION_BODIES}
    while left < end:
        right = left + timedelta(days=1)
        current = {name: position_loader(right, name) for name in STATION_BODIES}
        for name in STATION_BODIES:
            was_retrograde = previous[name]["speed"] < 0
            is_retrograde = current[name]["speed"] < 0
            if was_retrograde == is_retrograde:
                continue
            low, high = left, right
            # Keep high on the new-direction side, including exact boundaries.
            while (high - low).total_seconds() > 1:
                middle = low + (high - low) / 2
                if (position_loader(middle, name)["speed"] < 0) == was_retrograde:
                    low = middle
                else:
                    high = middle
            if start <= high < end:
                events.append({
                    "planet": name,
                    "date": high.date().isoformat(),
                    "local_datetime": high.isoformat(timespec="seconds"),
                    "turn": "retrograde" if is_retrograde else "direct",
                    "sign": position_loader(high, name)["sign"],
                    "source": "Swiss Ephemeris longitude velocity",
                    "time_precision": "within_1_second",
                })
        left, previous = right, current
    return sorted(events, key=lambda row: (row["local_datetime"], row["planet"]))
