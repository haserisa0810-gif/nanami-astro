from __future__ import annotations

"""四柱推命 計算（年/月/日/時） + 基本特徴量

実装済み:
- 年柱 / 月柱（節入り）
- 日柱 / 時柱
- 蔵干
- 十神
- 五行カウント（表示用 / 蔵干込み）
- 空亡（旬空）
- 十二運
- 身強スコア目安
- 大運（年干陰陽 × 出生時の性別 で順逆判定）

注意:
- 日替わり境界は流派差があるため option(day_change_at_23) で切替
- 性別は四柱推命の計算上は「出生時の性別」を使う
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any

from pathlib import Path
import swisseph as swe

def configure_ephemeris() -> int:
    ephe_dir = Path(__file__).resolve().parents[2] / "ephe"
    if ephe_dir.exists() and any(ephe_dir.glob("*.se1")):
        swe.set_ephe_path(str(ephe_dir))
        return swe.FLG_SWIEPH
    return swe.FLG_MOSEPH

def ephemeris_debug_info() -> dict:
    ephe_dir = Path(__file__).resolve().parents[2] / "ephe"
    files = sorted([p.name for p in ephe_dir.glob("*")]) if ephe_dir.exists() else []
    return {
        "ephe_dir": str(ephe_dir),
        "ephe_dir_exists": ephe_dir.exists(),
        "ephe_files": files,
        "has_se1": any(name.endswith(".se1") for name in files),
    }

# Swiss Ephemeris（節気計算に使用）
try:
    import swisseph as swe  # type: ignore
except Exception:  # pragma: no cover
    swe = None  # type: ignore


# 0=甲子 ... 59=癸亥
KANSHI_60 = [
    "甲子","乙丑","丙寅","丁卯","戊辰","己巳","庚午","辛未","壬申","癸酉",
    "甲戌","乙亥","丙子","丁丑","戊寅","己卯","庚辰","辛巳","壬午","癸未",
    "甲申","乙酉","丙戌","丁亥","戊子","己丑","庚寅","辛卯","壬辰","癸巳",
    "甲午","乙未","丙申","丁酉","戊戌","己亥","庚子","辛丑","壬寅","癸卯",
    "甲辰","乙巳","丙午","丁未","戊申","己酉","庚戌","辛亥","壬子","癸丑",
    "甲寅","乙卯","丙辰","丁巳","戊午","己未","庚申","辛酉","壬戌","癸亥",
]

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

# 月支（節入り）順：立春=寅月スタート
MONTH_BRANCHES = ["寅","卯","辰","巳","午","未","申","酉","戌","亥","子","丑"]

# 干の五行
STEM_ELEMENT = {
    "甲": "木", "乙": "木",
    "丙": "火", "丁": "火",
    "戊": "土", "己": "土",
    "庚": "金", "辛": "金",
    "壬": "水", "癸": "水",
}

# 支の本気（代表五行）
BRANCH_ELEMENT = {
    "子": "水", "丑": "土", "寅": "木", "卯": "木",
    "辰": "土", "巳": "火", "午": "火", "未": "土",
    "申": "金", "酉": "金", "戌": "土", "亥": "水",
}

# 蔵干（代表的な表）
HIDDEN_STEMS = {
    "子": ["癸"],
    "丑": ["己", "癸", "辛"],
    "寅": ["甲", "丙", "戊"],
    "卯": ["乙"],
    "辰": ["戊", "乙", "癸"],
    "巳": ["丙", "戊", "庚"],
    "午": ["丁", "己"],
    "未": ["己", "丁", "乙"],
    "申": ["庚", "壬", "戊"],
    "酉": ["辛"],
    "戌": ["戊", "辛", "丁"],
    "亥": ["壬", "甲"],
}

# 陰陽
YIN_YANG = {
    "甲": "陽", "乙": "陰",
    "丙": "陽", "丁": "陰",
    "戊": "陽", "己": "陰",
    "庚": "陽", "辛": "陰",
    "壬": "陽", "癸": "陰",
}

# 空亡（旬空）
KUBO_BY_JUN = [
    ["戌", "亥"],  # 甲子旬
    ["申", "酉"],  # 甲戌旬
    ["午", "未"],  # 甲申旬
    ["辰", "巳"],  # 甲午旬
    ["寅", "卯"],  # 甲辰旬
    ["子", "丑"],  # 甲寅旬
]

JUN_LABELS = ["甲子旬", "甲戌旬", "甲申旬", "甲午旬", "甲辰旬", "甲寅旬"]


# 時支の境界（ローカル時刻）
# 子:23:00-00:59, 丑:01:00-02:59, ... 戌:19:00-20:59, 亥:21:00-22:59
def _time_branch_index(local_dt: datetime) -> int:
    h = local_dt.hour
    m = local_dt.minute
    minutes = h * 60 + m

    # 子だけ日跨ぎなので特別扱い
    if minutes >= 23 * 60 or minutes < 1 * 60:
        return 0  # 子
    # それ以外は 2時間刻み：1:00-2:59 が丑(1) ... 21:00-22:59 が亥(11)
    # 1:00 を起点に 120分ブロック
    return 1 + ((minutes - 60) // 120)


def julian_day(dt_utc: datetime) -> float:
    """グレゴリオ暦のUTC datetime -> JD

    dt_utc は timezone.utc の aware datetime を想定
    """
    if dt_utc.tzinfo is None:
        raise ValueError("dt_utc must be timezone-aware (UTC).")
    dt_utc = dt_utc.astimezone(timezone.utc)

    y = dt_utc.year
    m = dt_utc.month
    d = dt_utc.day
    frac_day = (dt_utc.hour + (dt_utc.minute + dt_utc.second / 60) / 60) / 24

    if m <= 2:
        y -= 1
        m += 12

    A = y // 100
    B = 2 - A + (A // 4)

    jd = int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + B - 1524.5
    return jd + frac_day


def _dt_utc_to_jdut(dt_utc: datetime) -> float:
    """UTC datetime -> Swiss Ephemeris 用の JD(UT)"""
    if swe is None:
        raise RuntimeError("pyswisseph が読み込めません（requirements.txt を確認）")
    dt_utc = dt_utc.astimezone(timezone.utc)
    hour = dt_utc.hour + (dt_utc.minute + dt_utc.second / 60) / 60
    return float(swe.julday(dt_utc.year, dt_utc.month, dt_utc.day, hour))


def _sun_lon_deg(jd_ut: float) -> float:
    """太陽黄経（0-360）"""
    if swe is None:
        raise RuntimeError("pyswisseph が読み込めません（requirements.txt を確認）")
    flags = configure_ephemeris()
    xx, _ = swe.calc_ut(jd_ut, swe.SUN, flags)
    return float(xx[0]) % 360.0


def _angle_diff_deg(a: float, b: float) -> float:
    """角度差 a-b を [-180, 180) に正規化"""
    return ((a - b + 180.0) % 360.0) - 180.0


def _find_solar_longitude_crossing(
    *,
    target_lon_deg: float,
    approx_dt_utc: datetime,
    search_days: int = 7,
) -> datetime:
    """太陽黄経が target_lon_deg を横切る瞬間を近傍探索して返す（UTC）。"""
    if swe is None:
        raise RuntimeError("pyswisseph が読み込めません（requirements.txt を確認）")

    target = target_lon_deg % 360.0
    approx = approx_dt_utc.astimezone(timezone.utc)

    step = timedelta(hours=6)
    start = approx - timedelta(days=search_days)
    end = approx + timedelta(days=search_days)

    t = start
    jd0 = _dt_utc_to_jdut(t)
    f0 = _angle_diff_deg(_sun_lon_deg(jd0), target)

    while t < end:
        t1 = t + step
        jd1 = _dt_utc_to_jdut(t1)
        f1 = _angle_diff_deg(_sun_lon_deg(jd1), target)

        if f0 == 0:
            return t

        if (f0 < 0 <= f1) or (f0 > 0 >= f1):
            lo_t, hi_t = t, t1
            lo_f, hi_f = f0, f1
            for _ in range(60):
                mid_t = lo_t + (hi_t - lo_t) / 2
                mid_jd = _dt_utc_to_jdut(mid_t)
                mid_f = _angle_diff_deg(_sun_lon_deg(mid_jd), target)
                if abs((hi_t - lo_t).total_seconds()) <= 1:
                    return mid_t
                if (lo_f < 0 <= mid_f) or (lo_f > 0 >= mid_f):
                    hi_t, hi_f = mid_t, mid_f
                else:
                    lo_t, lo_f = mid_t, mid_f
            return lo_t

        t, f0 = t1, f1

    raise RuntimeError("節気計算に失敗しました（探索範囲不足の可能性）")


def _lichun_utc_for_year(year: int, tz_name: str) -> datetime:
    """指定年の立春（太陽黄経315°）の瞬間を UTC で返す。"""
    tz = ZoneInfo(tz_name)
    approx_local = datetime(year, 2, 4, 12, 0, 0, tzinfo=tz)
    return _find_solar_longitude_crossing(
        target_lon_deg=315.0,
        approx_dt_utc=approx_local.astimezone(timezone.utc),
    )


def _month_index_from_sun_lon(sun_lon_deg: float) -> int:
    """太陽黄経から節入り月インデックス（0=寅月..11=丑月）"""
    return int(((sun_lon_deg - 315.0) % 360.0) // 30.0)


def _year_kanshi_from_effective_year(y: int) -> str:
    idx = (y - 4) % 60
    return KANSHI_60[idx]


def _month_kanshi_from_year_stem_and_index(year_stem: str, month_index: int) -> str:
    start_map = {
        "甲": "丙", "己": "丙",
        "乙": "戊", "庚": "戊",
        "丙": "庚", "辛": "庚",
        "丁": "壬", "壬": "壬",
        "戊": "甲", "癸": "甲",
    }
    if year_stem not in start_map:
        raise ValueError(f"Unknown year stem: {year_stem}")
    start = start_map[year_stem]
    stem = STEMS[(STEMS.index(start) + month_index) % 10]
    branch = MONTH_BRANCHES[month_index]
    return stem + branch


def _element_generate(elem: str) -> str:
    order = ["木", "火", "土", "金", "水"]
    return order[(order.index(elem) + 1) % 5]


def _element_control(elem: str) -> str:
    control = {"木": "土", "土": "水", "水": "火", "火": "金", "金": "木"}
    return control[elem]


def ten_god(day_stem: str, other_stem: str) -> str:
    """日干から見た十神（通変星）"""
    dm_elem = STEM_ELEMENT[day_stem]
    ot_elem = STEM_ELEMENT[other_stem]
    dm_yy = YIN_YANG[day_stem]
    ot_yy = YIN_YANG[other_stem]
    same_polarity = (dm_yy == ot_yy)

    if ot_elem == dm_elem:
        return "比肩" if same_polarity else "劫財"
    if _element_generate(dm_elem) == ot_elem:
        return "食神" if same_polarity else "傷官"
    if _element_control(dm_elem) == ot_elem:
        return "偏財" if same_polarity else "正財"
    if _element_control(ot_elem) == dm_elem:
        return "偏官" if same_polarity else "正官"
    if _element_generate(ot_elem) == dm_elem:
        return "偏印" if same_polarity else "印綬"
    return "不明"


def _effective_local_date_for_day_pillar(
    local_dt: datetime,
    *,
    day_change_at_23: bool,
) -> tuple[datetime.date, str]:
    """日柱判定に使うローカル日付を返す。

    現在のWeb UI上のオプションは、利用者の期待値に合わせて
    「子刻後半（00:00-00:59）を前日扱いにする」挙動に寄せる。

    - OFF: 暦日どおり（00:00切替）
    - ON : 00:00-00:59 は前日の日柱を使う

    これにより 1975-09-15 00:02 は
    - OFF -> 1975-09-15 扱い（甲子）
    - ON  -> 1975-09-14 扱い（癸亥）
    となる。
    """
    local_date = local_dt.date()
    rule_label = "00:00切替"

    if day_change_at_23 and local_dt.hour == 0:
        local_date = (datetime(local_date.year, local_date.month, local_date.day) - timedelta(days=1)).date()
        rule_label = "子刻後半(00:00-00:59)を前日扱い"
    elif day_change_at_23:
        rule_label = "子刻考慮ON"

    return local_date, rule_label


def day_kanshi_from_birth(
    birth_dt: datetime,
    tz_name: str = "Asia/Tokyo",
    day_change_at_23: bool = False,
) -> str:
    """日干支（四柱推命NEXT式）

    int((J + 0.5 + 50) % 60)
    ここで J は「日柱判定に使う日の0:00(ローカル)をUTCに直したJD」を使う。
    """
    tz = ZoneInfo(tz_name)
    if birth_dt.tzinfo is None:
        # 入力がnaiveなら tz を付与（ローカル入力想定）
        local_dt = birth_dt.replace(tzinfo=tz)
    else:
        local_dt = birth_dt.astimezone(tz)

    local_date, _rule_label = _effective_local_date_for_day_pillar(
        local_dt,
        day_change_at_23=day_change_at_23,
    )

    local_midnight = datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0, tzinfo=tz)
    utc_midnight = local_midnight.astimezone(timezone.utc)
    J = julian_day(utc_midnight)

    idx = int((J + 0.5 + 50) % 60)
    return KANSHI_60[idx]


def hour_kanshi_from_day_stem_and_time(
    day_stem: str,
    birth_dt: datetime,
    tz_name: str = "Asia/Tokyo",
) -> str:
    """日干とローカル時刻から時柱（時干支）を算出。"""
    tz = ZoneInfo(tz_name)
    local_dt = birth_dt.replace(tzinfo=tz) if birth_dt.tzinfo is None else birth_dt.astimezone(tz)

    b_idx = _time_branch_index(local_dt)
    branch = BRANCHES[b_idx]

    # 甲己日→子刻は甲、乙庚日→子刻は丙、丙辛日→子刻は戊、丁壬日→子刻は庚、戊癸日→子刻は壬
    start_stem_map = {
        "甲": "甲", "己": "甲",
        "乙": "丙", "庚": "丙",
        "丙": "戊", "辛": "戊",
        "丁": "庚", "壬": "庚",
        "戊": "壬", "癸": "壬",
    }
    if day_stem not in start_stem_map:
        raise ValueError(f"Unknown day stem: {day_stem}")

    start = start_stem_map[day_stem]
    start_idx = STEMS.index(start)
    stem = STEMS[(start_idx + b_idx) % 10]
    return stem + branch


@dataclass(frozen=True)
class Pillars:
    day: str
    hour: str


@dataclass(frozen=True)
class Pillars4:
    year: str
    month: str
    day: str
    hour: str


def pillars_day_hour(
    birth_dt: datetime,
    tz_name: str = "Asia/Tokyo",
    day_change_at_23: bool = False,
) -> Pillars:
    day = day_kanshi_from_birth(birth_dt, tz_name=tz_name, day_change_at_23=day_change_at_23)
    day_stem = day[0]
    hour = hour_kanshi_from_day_stem_and_time(day_stem, birth_dt, tz_name=tz_name)
    return Pillars(day=day, hour=hour)


def year_month_pillars(
    birth_dt: datetime,
    tz_name: str = "Asia/Tokyo",
) -> tuple[str, str, dict[str, Any]]:
    """年柱・月柱（節入り）。

    戻り値: (year_kanshi, month_kanshi, debug)
    debug には立春時刻や太陽黄経などの内部情報を入れる。
    """
    tz = ZoneInfo(tz_name)
    local_dt = birth_dt.replace(tzinfo=tz) if birth_dt.tzinfo is None else birth_dt.astimezone(tz)
    utc_dt = local_dt.astimezone(timezone.utc)

    if swe is None:
        raise RuntimeError("pyswisseph が読み込めません（requirements.txt を確認）")

    # 立春（年境界）
    lichun_utc = _lichun_utc_for_year(local_dt.year, tz_name)
    lichun_local = lichun_utc.astimezone(tz)
    effective_year = local_dt.year if local_dt >= lichun_local else (local_dt.year - 1)

    year_kanshi = _year_kanshi_from_effective_year(effective_year)

    # 月柱（節入り月）：太陽黄経で判定
    sun_lon = _sun_lon_deg(_dt_utc_to_jdut(utc_dt))
    month_index = _month_index_from_sun_lon(sun_lon)
    month_kanshi = _month_kanshi_from_year_stem_and_index(year_kanshi[0], month_index)

    debug = {
        "tz_name": tz_name,
        "local_datetime": local_dt.isoformat(),
        "lichun_local": lichun_local.isoformat(),
        "effective_year": effective_year,
        "sun_lon_deg": round(sun_lon, 6),
        "month_index": month_index,
        "month_branch": MONTH_BRANCHES[month_index],
    }
    return year_kanshi, month_kanshi, debug


def pillars4(
    birth_dt: datetime,
    tz_name: str = "Asia/Tokyo",
    day_change_at_23: bool = False,
) -> Pillars4:
    """四柱（年/月/日/時）をまとめて算出。"""
    y, m, _dbg = year_month_pillars(birth_dt, tz_name=tz_name)
    day = day_kanshi_from_birth(birth_dt, tz_name=tz_name, day_change_at_23=day_change_at_23)
    hour = hour_kanshi_from_day_stem_and_time(day[0], birth_dt, tz_name=tz_name)
    return Pillars4(year=y, month=m, day=day, hour=hour)


def _empty_five_counts() -> dict[str, int]:
    return {"木": 0, "火": 0, "土": 0, "金": 0, "水": 0}


def _five_elements_counts_visible(p: Pillars4) -> dict[str, int]:
    """表示用五行カウント（天干+地支のみ）。"""
    counts = _empty_five_counts()
    for stem in (p.year[0], p.month[0], p.day[0], p.hour[0]):
        counts[STEM_ELEMENT.get(stem, "土")] += 1
    for br in (p.year[1], p.month[1], p.day[1], p.hour[1]):
        counts[BRANCH_ELEMENT.get(br, "土")] += 1
    return counts


def _five_elements_counts_with_hidden(p: Pillars4) -> dict[str, int]:
    """蔵干込み五行カウント。"""
    counts = _five_elements_counts_visible(p)
    for br in (p.year[1], p.month[1], p.day[1], p.hour[1]):
        for hs in HIDDEN_STEMS.get(br, []):
            counts[STEM_ELEMENT.get(hs, "土")] += 1
    return counts


def _five_elements_counts(p: Pillars4) -> dict[str, Any]:
    return {
        "visible": _five_elements_counts_visible(p),
        "with_hidden_stems": _five_elements_counts_with_hidden(p),
    }


def _ten_gods_for_pillars(p: Pillars4) -> dict[str, Any]:
    dm = p.day[0]
    out: dict[str, Any] = {
        "day_master": dm,
        "pillars": {
            "year": {"stem": p.year[0], "ten_god": ten_god(dm, p.year[0])},
            "month": {"stem": p.month[0], "ten_god": ten_god(dm, p.month[0])},
            "hour": {"stem": p.hour[0], "ten_god": ten_god(dm, p.hour[0])},
        },
        "hidden_stems": {},
    }
    for key, br in (
        ("year", p.year[1]),
        ("month", p.month[1]),
        ("day", p.day[1]),
        ("hour", p.hour[1]),
    ):
        hs = HIDDEN_STEMS.get(br, [])
        out["hidden_stems"][key] = [{"stem": s, "ten_god": ten_god(dm, s)} for s in hs]
    return out



# =========================
# 追加：十二運 / 身強スコア / 大運
# =========================

# 十二運（Wikipediaの表に準拠：胎,養,長生,...,絶）
# 胎 養 長生 沐浴 冠帯 建禄 帝旺 衰 病 死 墓 絶
_TWELVE_FORTUNE_TABLE: dict[str, list[str]] = {
    "甲": ["酉","戌","亥","子","丑","寅","卯","辰","巳","午","未","申"],
    "乙": ["申","未","午","巳","辰","卯","寅","丑","子","亥","戌","酉"],
    "丙": ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"],
    "丁": ["亥","戌","酉","申","未","午","巳","辰","卯","寅","丑","子"],
    # 土は流派差があるが、本実装は「火土同根」で丙/丁と同形を採用（よく使われる形）
    "戊": ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"],
    "己": ["亥","戌","酉","申","未","午","巳","辰","卯","寅","丑","子"],
    "庚": ["卯","辰","巳","午","未","申","酉","戌","亥","子","丑","寅"],
    "辛": ["寅","丑","子","亥","戌","酉","申","未","午","巳","辰","卯"],
    "壬": ["午","未","申","酉","戌","亥","子","丑","寅","卯","辰","巳"],
    "癸": ["巳","辰","卯","寅","丑","子","亥","戌","酉","申","未","午"],
}

_TWELVE_FORTUNE_STAGES = ["胎","養","長生","沐浴","冠帯","建禄","帝旺","衰","病","死","墓","絶"]

# 「強い/弱い」断定を避けるため、ステージにエネルギー値(目安)を付与
# （帝旺を最大に近く、絶を最小に近くする単調なスケール）
_TWELVE_FORTUNE_ENERGY = {
    "胎": 2, "養": 3, "長生": 5, "沐浴": 4, "冠帯": 6, "建禄": 8,
    "帝旺": 9, "衰": 7, "病": 4, "死": 3, "墓": 3, "絶": 1,
}


def twelve_fortune(day_stem: str, branch: str) -> str:
    """日干×地支 → 十二運（胎〜絶）"""
    table = _TWELVE_FORTUNE_TABLE.get(day_stem)
    if not table or branch not in table:
        return "不明"
    idx = table.index(branch)
    return _TWELVE_FORTUNE_STAGES[idx]


def _twelve_fortune_for_pillars(p4: Pillars4) -> dict[str, str]:
    dm = p4.day[0]
    return {
        "year": twelve_fortune(dm, p4.year[1]),
        "month": twelve_fortune(dm, p4.month[1]),
        "day": twelve_fortune(dm, p4.day[1]),
        "hour": twelve_fortune(dm, p4.hour[1]),
    }


def _parse_gender(payload_gender: Any) -> str:
    """性別表記を male / female に正規化する。未指定時は female。"""
    g = (str(payload_gender) if payload_gender is not None else "").strip().lower()
    if g in ("male", "m", "man", "男性", "男"):
        return "male"
    if g in ("female", "f", "woman", "女性", "女", "default"):
        return "female"
    return "female"


def _is_yang_stem(stem: str) -> bool:
    # 甲丙戊庚壬 が陽
    return stem in ("甲", "丙", "戊", "庚", "壬")


def _daiun_direction_from_year_stem(year_stem: str, gender: str) -> str:
    """年干陰陽 × 出生時の性別で大運方向を返す。"""
    is_yang = _is_yang_stem(year_stem)
    if is_yang:
        return "forward" if gender == "male" else "reverse"
    return "reverse" if gender == "male" else "forward"


def _month_branch_element(month_branch: str) -> str:
    return BRANCH_ELEMENT.get(month_branch, "土")


def _calc_strength_score(
    p4: Pillars4,
) -> dict[str, Any]:
    """身強身弱を“断定せず”スコア化するための材料。

    ここでは厳密な格局判定はせず、次の根拠を点数化する：
    - 得令（季節）：月支の五行が日干を助ける/削ぐ
    - 通根：日干と同五行の蔵干がどれだけ地支にあるか（特に月支を重視）
    - 透干：通根に近い要素が天干に現れているか

    返す score は「目安（0-100）」。
    """
    dm = p4.day[0]
    dm_elem = STEM_ELEMENT[dm]
    month_br = p4.month[1]
    month_elem = _month_branch_element(month_br)

    breakdown: list[dict[str, Any]] = []
    score = 50  # 中央から

    # 1) 得令（季節）
    # - 月支五行が日干五行と同じ：+12
    # - 月支五行が日干を生じる：+8
    # - 月支五行が日干に剋される：-6（消耗）
    # - 月支五行が日干を剋す：-12（圧力）
    if month_elem == dm_elem:
        score += 12
        breakdown.append({"name": "得令(同気)", "delta": 12, "detail": f"月支({month_br})={month_elem} が日干({dm})={dm_elem} と同じ"})
    elif _element_generate(month_elem) == dm_elem:
        score += 8
        breakdown.append({"name": "得令(相生)", "delta": 8, "detail": f"月支({month_br})={month_elem} が日干({dm})={dm_elem} を生じる"})
    elif _element_control(dm_elem) == month_elem:
        score -= 6
        breakdown.append({"name": "得令(泄耗)", "delta": -6, "detail": f"日干({dm})={dm_elem} が月支({month_br})={month_elem} を剋す側で消耗しやすい"})
    elif _element_control(month_elem) == dm_elem:
        score -= 12
        breakdown.append({"name": "得令(相剋)", "delta": -12, "detail": f"月支({month_br})={month_elem} が日干({dm})={dm_elem} を剋す"})
    else:
        breakdown.append({"name": "得令(中立)", "delta": 0, "detail": f"月支({month_br})={month_elem} と日干({dm})={dm_elem} の直接関係が弱い"})

    # 2) 通根（同五行の蔵干）
    roots = []
    for pillar_name, br in (("month", p4.month[1]), ("day", p4.day[1]), ("hour", p4.hour[1]), ("year", p4.year[1])):
        hs = HIDDEN_STEMS.get(br, [])
        for s in hs:
            if STEM_ELEMENT.get(s) == dm_elem:
                roots.append({"pillar": pillar_name, "branch": br, "hidden_stem": s})

    # 重み：月>日>時>年
    w = {"month": 10, "day": 6, "hour": 4, "year": 3}
    root_points = 0
    for r in roots:
        root_points += w.get(r["pillar"], 2)
    if roots:
        score += min(25, root_points)
        breakdown.append({"name": "通根", "delta": min(25, root_points), "detail": f"同五行の蔵干が {len(roots)} 件", "roots": roots})
    else:
        breakdown.append({"name": "通根", "delta": 0, "detail": "同五行の蔵干が見当たらない（通根弱めの可能性）"})

    # 3) 透干（同五行が天干に見えるか）
    visible = []
    for pillar_name, stem in (("year", p4.year[0]), ("month", p4.month[0]), ("hour", p4.hour[0])):
        if STEM_ELEMENT.get(stem) == dm_elem:
            visible.append({"pillar": pillar_name, "stem": stem})
    if visible:
        delta = 8 + 2 * max(0, len(visible) - 1)
        delta = min(delta, 14)
        score += delta
        breakdown.append({"name": "透干", "delta": delta, "detail": f"同五行の天干が {len(visible)} 件", "visible": visible})
    else:
        breakdown.append({"name": "透干", "delta": 0, "detail": "同五行の天干は少なめ"})

    # 4) 十二運（四柱の地支に出る“勢い”を軽く加点）
    tf = _twelve_fortune_for_pillars(p4)
    tf_points = 0
    for k, stage in tf.items():
        tf_points += _TWELVE_FORTUNE_ENERGY.get(stage, 0)
    # 4柱合計を 0-36 くらい → 0-12 に圧縮
    delta = int(round(tf_points / 3))
    score += delta
    breakdown.append({"name": "十二運(合算目安)", "delta": delta, "detail": tf})

    # clamp
    score = max(0, min(100, int(round(score))))

    # “身強/身弱”の断定は避け、レンジラベルだけ付ける
    if score >= 70:
        label = "強め寄り（目安）"
    elif score <= 40:
        label = "弱め寄り（目安）"
    else:
        label = "中庸寄り（目安）"

    return {"score": score, "label": label, "breakdown": breakdown}


def _sexagenary_index(kanshi: str) -> int:
    try:
        return KANSHI_60.index(kanshi)
    except ValueError:
        return -1


def _jieqi_boundary_utc_for_birth_month(
    birth_local: datetime,
    tz_name: str,
) -> tuple[datetime, datetime]:
    """出生が属する節入り区間の境界（前/次）を返す。

    - 月柱判定に使う「30度刻み境界」を“節入り”として扱う。
    - 近似でも良いので、AIに渡す際の根拠は debug に残す。
    """
    tz = ZoneInfo(tz_name)
    local_dt = birth_local.astimezone(tz)
    utc_dt = local_dt.astimezone(timezone.utc)
    jdut = _dt_utc_to_jdut(utc_dt)

    sun_lon = _sun_lon_deg(jdut)
    cur_idx = _month_index_from_sun_lon(sun_lon)

    # その月の開始境界と次境界の太陽黄経ターゲット
    start_lon = (315 + 30 * cur_idx) % 360
    next_lon = (start_lon + 30) % 360

    prev_utc = _solve_time_for_sun_lon_near(jdut, target_lon=start_lon, direction=-1)
    next_utc = _solve_time_for_sun_lon_near(jdut, target_lon=next_lon, direction=+1)
    return prev_utc, next_utc


def _solve_time_for_sun_lon_near(jd_start: float, *, target_lon: float, direction: int) -> datetime:
    """太陽黄経が target_lon になる時刻(UTC)を近傍探索（簡易二分探索）。

    direction:
      +1: 未来方向に探す
      -1: 過去方向に探す
    """
    # 探索幅：最大 40日（節気は約15日〜16日間隔だが30度境界は約30日）
    step_days = 1.0
    jd0 = jd_start
    lon0 = _sun_lon_deg(jd0)

    # 角度差を -180..180 に正規化
    def ang_diff(a: float, b: float) -> float:
        d = (a - b) % 360.0
        if d > 180:
            d -= 360
        return d

    # bracket
    jd_a = jd0
    jd_b = jd0
    diff_a = ang_diff(lon0, target_lon)

    for _ in range(50):
        jd_b = jd_a + direction * step_days
        diff_b = ang_diff(_sun_lon_deg(jd_b), target_lon)
        # sign change or close enough
        if diff_a == 0 or diff_b == 0 or (diff_a > 0) != (diff_b > 0):
            break
        jd_a = jd_b
        diff_a = diff_b
        step_days *= 1.2
        if step_days > 40:
            break

    lo, hi = (jd_a, jd_b) if jd_a < jd_b else (jd_b, jd_a)

    # binary refine
    for _ in range(40):
        mid = (lo + hi) / 2
        diff_mid = ang_diff(_sun_lon_deg(mid), target_lon)
        diff_lo = ang_diff(_sun_lon_deg(lo), target_lon)
        if diff_mid == 0:
            lo = hi = mid
            break
        if (diff_lo > 0) != (diff_mid > 0):
            hi = mid
        else:
            lo = mid

    jd_final = (lo + hi) / 2
    # jdut -> utc datetime
    return _jdut_to_datetime_utc(jd_final)


def _jdut_to_datetime_utc(jdut: float) -> datetime:
    # JDUT -> Gregorian UTC datetime
    # この用途は“節入り差分”なので秒精度は不要。簡易変換。
    # 参考：JD 2440587.5 が Unix epoch (1970-01-01 00:00 UTC)
    unix = (jdut - 2440587.5) * 86400.0
    return datetime.fromtimestamp(unix, tz=timezone.utc)


def kubo_from_day_kanshi(day_kanshi: str) -> dict[str, Any]:
    idx = _sexagenary_index(day_kanshi)
    if idx < 0:
        return {
            "day_kanshi": day_kanshi,
            "jun_index": None,
            "jun_label": None,
            "empty_branches": [],
            "error": "day_kanshi not in KANSHI_60",
        }
    jun_index = idx // 10
    return {
        "day_kanshi": day_kanshi,
        "jun_index": jun_index,
        "jun_label": JUN_LABELS[jun_index],
        "empty_branches": KUBO_BY_JUN[jun_index],
    }


def kubo_for_pillars(p4: Pillars4) -> dict[str, Any]:
    base = kubo_from_day_kanshi(p4.day)
    empty = set(base.get("empty_branches", []))
    hits = {
        "year": p4.year[1] in empty,
        "month": p4.month[1] in empty,
        "day": p4.day[1] in empty,
        "hour": p4.hour[1] in empty,
    }
    return {
        **base,
        "hits": hits,
        "hit_pillars": [name for name, hit in hits.items() if hit],
        "pillars": {
            "year": {"kanshi": p4.year, "branch": p4.year[1]},
            "month": {"kanshi": p4.month, "branch": p4.month[1]},
            "day": {"kanshi": p4.day, "branch": p4.day[1]},
            "hour": {"kanshi": p4.hour, "branch": p4.hour[1]},
        },
    }


def _calc_daiun(
    birth_dt: datetime,
    p4: Pillars4,
    *,
    tz_name: str,
    gender: str,
) -> dict[str, Any]:
    """大運（10年運）を算出。

    - 順逆：年干の陰陽 × 出生時の性別
    - 起運：出生〜節入り差分の近似換算（3日=1年, 1日=4ヶ月）
    """
    tz = ZoneInfo(tz_name)
    local_birth = birth_dt.replace(tzinfo=tz) if birth_dt.tzinfo is None else birth_dt.astimezone(tz)

    gender_norm = _parse_gender(gender)
    year_stem = p4.year[0]
    direction = _daiun_direction_from_year_stem(year_stem, gender_norm)
    forward = direction == "forward"

    prev_jieqi_utc, next_jieqi_utc = _jieqi_boundary_utc_for_birth_month(local_birth, tz_name)
    birth_utc = local_birth.astimezone(timezone.utc)

    if forward:
        delta = next_jieqi_utc - birth_utc
        ref = next_jieqi_utc
    else:
        delta = birth_utc - prev_jieqi_utc
        ref = prev_jieqi_utc

    total_days = max(0.0, delta.total_seconds() / 86400.0)
    years = int(total_days // 3)
    rem_days = total_days - years * 3
    months = int(rem_days) * 4 + int(round((rem_days - int(rem_days)) * 4))
    years += months // 12
    months = months % 12

    start_age = {"years": years, "months": months}
    start_age_text = f"{years}年{months}ヶ月"

    m_idx = _sexagenary_index(p4.month)
    if m_idx < 0:
        return {
            "direction": direction,
            "gender": gender_norm,
            "start_age": start_age,
            "start_age_text": start_age_text,
            "note": "month_kanshi not in KANSHI_60",
        }

    step = 1 if forward else -1
    luck0 = (m_idx + step) % 60

    items = []
    age0 = years + months / 12.0
    start_year = local_birth.year + years + (1 if months > 0 else 0)
    for i in range(10):
        k = KANSHI_60[(luck0 + step * i) % 60]
        items.append({
            "index": i + 1,
            "kanshi": k,
            "age_from": round(age0 + 10 * i, 2),
            "age_to": round(age0 + 10 * (i + 1), 2),
            "approx_start_year": start_year + 10 * i,
        })

    debug = {
        "gender": gender_norm,
        "year_stem": year_stem,
        "year_stem_yang": _is_yang_stem(year_stem),
        "direction_rule": "year_stem_yinyang_x_birth_gender",
        "jieqi_ref_utc": ref.isoformat(),
        "delta_days": round(total_days, 6),
    }

    return {
        "direction": direction,
        "gender": gender_norm,
        "start_age": start_age,
        "start_age_text": start_age_text,
        "items": items,
        "debug": debug,
    }


def _pillar_dict(kanshi: str) -> dict[str, Any]:
    return {
        "stem": kanshi[0],
        "branch": kanshi[1],
        "kanshi": kanshi,
    }


def _build_unknowns(payload: dict[str, Any], *, day_change_at_23: bool) -> list[str]:
    unknowns: list[str] = []
    if payload.get("hour") in (None, "", "不明"):
        unknowns.append("出生時刻が未入力のため 12:00 を仮置きして計算")
    if payload.get("minute") in (None, "", "不明"):
        unknowns.append("出生分が未入力のため 00 分として計算")
    if day_change_at_23:
        unknowns.append("日替わり境界は 23:00 切替設定を使用")
    else:
        unknowns.append("日替わり境界は 00:00 切替設定を使用")
    unknowns.append("大運開始年齢は簡易換算（3日=1年, 1日=4ヶ月）")
    return unknowns


def _build_structure_report(
    p4: Pillars4,
    strength: dict[str, Any],
    five: dict[str, Any],
    ten: dict[str, Any],
    tf: dict[str, str],
    daiun: dict[str, Any],
) -> dict[str, Any]:
    dm = p4.day[0]
    dm_elem = STEM_ELEMENT[dm]
    month_branch = p4.month[1]
    month_element = _month_branch_element(month_branch)

    if month_element == dm_elem:
        relation = "same_element"
    elif _element_generate(month_element) == dm_elem:
        relation = "generated_by_month"
    elif _element_control(month_element) == dm_elem:
        relation = "controlled_by_month"
    elif _element_control(dm_elem) == month_element:
        relation = "day_master_controls_month"
    elif _element_generate(dm_elem) == month_element:
        relation = "day_master_generates_month"
    else:
        relation = "neutral"

    hidden = ten.get("hidden_stems", {}) if isinstance(ten, dict) else {}
    roots_count = 0
    for items in hidden.values():
        if isinstance(items, list):
            for item in items:
                stem = item.get("stem") if isinstance(item, dict) else None
                if stem and STEM_ELEMENT.get(stem) == dm_elem:
                    roots_count += 1

    visible_same = 0
    for pillar_name in ("year", "month", "hour"):
        pillar = ten.get("pillars", {}).get(pillar_name, {}) if isinstance(ten, dict) else {}
        stem = pillar.get("stem") if isinstance(pillar, dict) else None
        if stem and STEM_ELEMENT.get(stem) == dm_elem:
            visible_same += 1

    tf_total = sum(_TWELVE_FORTUNE_ENERGY.get(stage, 0) for stage in tf.values())

    dominant_features: list[str] = []
    visible_counts = five.get("visible", {}) if isinstance(five, dict) else {}
    if isinstance(visible_counts, dict) and visible_counts:
        ordered = sorted(visible_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        top_elem, top_count = ordered[0]
        bottom_elem, bottom_count = sorted(visible_counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
        dominant_features.append(f"visible五行で {top_elem} が相対的に多い ({top_count})")
        dominant_features.append(f"visible五行で {bottom_elem} が相対的に少ない ({bottom_count})")
    dominant_features.append(f"月支は {month_branch} ({month_element})")
    dominant_features.append(f"大運方向は {daiun.get('direction')}")

    notes = [
        "年柱は立春基準、月柱は節入り基準で判定",
        "AI翻訳前の構造材料として整理されたデータ",
    ]

    return {
        "day_master": dm,
        "day_master_element": dm_elem,
        "seasonal_context": {
            "month_branch": month_branch,
            "month_element": month_element,
            "relation_to_day_master": relation,
        },
        "strength_index": {
            "score": strength.get("score"),
            "label": strength.get("label"),
            "breakdown": strength.get("breakdown", []),
        },
        "support_signals": {
            "roots_count": roots_count,
            "visible_same_element_count": visible_same,
            "twelve_fortune_total": tf_total,
        },
        "dominant_features": dominant_features,
        "notes": notes,
    }


def calc_shichusuimei_from_payload(
    payload: dict[str, Any],
    *,
    tz_name: str = "Asia/Tokyo",
    day_change_at_23: bool = False,
) -> dict[str, Any]:
    """routes.py の payload から四柱推命結果を作る。

    返却は従来互換キーを残しつつ、StructurePayload 寄りの
    input / unknowns / normalized_data / structure_report を含める。
    """
    raw_hour = payload.get("hour", 12)
    raw_minute = payload.get("minute", 0)
    hour = 12 if raw_hour in (None, "") else int(raw_hour)
    minute = 0 if raw_minute in (None, "") else int(raw_minute)
    dt = datetime(
        int(payload["year"]),
        int(payload["month"]),
        int(payload["day"]),
        hour,
        minute,
        0,
    )

    p4 = pillars4(dt, tz_name=tz_name, day_change_at_23=day_change_at_23)
    _, _, ym_debug = year_month_pillars(dt, tz_name=tz_name)

    five = _five_elements_counts(p4)
    ten = _ten_gods_for_pillars(p4)
    tf = _twelve_fortune_for_pillars(p4)
    strength = _calc_strength_score(p4)
    daiun = _calc_daiun(dt, p4, tz_name=tz_name, gender=payload.get("gender"))
    kubo = kubo_for_pillars(p4)

    hidden_all = {
        "year": HIDDEN_STEMS.get(p4.year[1], []),
        "month": HIDDEN_STEMS.get(p4.month[1], []),
        "day": HIDDEN_STEMS.get(p4.day[1], []),
        "hour": HIDDEN_STEMS.get(p4.hour[1], []),
    }
    hidden_main = {k: (v[0] if v else None) for k, v in hidden_all.items()}
    gender_norm = _parse_gender(payload.get("gender"))
    unknowns = _build_unknowns(payload, day_change_at_23=day_change_at_23)

    normalized_data = {
        "pillars": {
            "year": _pillar_dict(p4.year),
            "month": _pillar_dict(p4.month),
            "day": _pillar_dict(p4.day),
            "hour": _pillar_dict(p4.hour),
        },
        "hidden_stems": hidden_all,
        "main_hidden_stems": hidden_main,
        "ten_gods": ten,
        "five_elements": {
            **five,
            "display_policy": "visible",
        },
        "twelve_fortune": tf,
        "kubo": kubo,
        "daiun": {
            **daiun,
            "calc_mode": "approx",
        },
    }

    structure_report = _build_structure_report(
        p4=p4,
        strength=strength,
        five=five,
        ten=ten,
        tf=tf,
        daiun=daiun,
    )

    return {
        "module": "shichusuimei",
        "system": "shichusuimei",
        "ephemeris": ephemeris_debug_info(),
        "engine_version": "0.2.7",
        "generated_at": datetime.now(ZoneInfo(tz_name)).isoformat(),
        "input": {
            "raw": {
                "year": int(payload["year"]),
                "month": int(payload["month"]),
                "day": int(payload["day"]),
                "hour": hour,
                "minute": minute,
                "gender": gender_norm,
                "city": payload.get("city"),
                "lat": payload.get("lat"),
                "lng": payload.get("lng"),
            },
            "assumptions": {
                "tz_name": tz_name,
                "day_change_at_23": day_change_at_23,
                "day_boundary_rule": "子刻後半(00:00-00:59)を前日扱い" if day_change_at_23 else "00:00切替",
                "year_boundary_rule": "lichun(315deg)",
                "month_boundary_rule": "solar_terms(315deg+30deg_step)",
                "daiun_start_mode": "approx_days_to_years",
            },
        },
        "unknowns": unknowns,
        "normalized_data": normalized_data,
        "structure_report": structure_report,
        "debug": {
            "year_month": ym_debug,
            "daiun": daiun.get("debug", {}),
        },
        # backward compatibility for current templates / prompts
        "pillars": asdict(p4),
        "day_master": p4.day[0],
        "raw": {
            "pillars": asdict(p4),
            "options": {
                "tz_name": tz_name,
                "day_change_at_23": day_change_at_23,
                "day_boundary_rule": "子刻後半(00:00-00:59)を前日扱い" if day_change_at_23 else "00:00切替",
                "year_boundary": "lichun(315deg)",
                "month_boundary": "solar_terms(315deg+30deg_step)",
            },
            "debug": {"year_month": ym_debug},
        },
        "features": {
            "five_elements": normalized_data["five_elements"],
            "ten_gods": ten,
            "twelve_fortune": tf,
            "strength": strength,
            "daiun": normalized_data["daiun"],
            "hidden_stems": hidden_all,
            "main_hidden_stems": hidden_main,
            "kubo": kubo,
        },
        "summary": {
            "year_kanshi": p4.year,
            "month_kanshi": p4.month,
            "day_kanshi": p4.day,
            "hour_kanshi": p4.hour,
        },
    }
