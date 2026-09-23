"""zodiac_context: 月次トランジットYAMLに「星座ごとの当たり方」を追加する

入力: transits dict（target_month, major_aspects, daily_snapshots を含む）
出力: {"Aries": {...}, ...} を transits["zodiac_context"] に入れる
天体計算は行わない。daily_snapshots の sign / degree だけを使う。
"""
from datetime import date, timedelta

SIGNS = ["Ari", "Tau", "Gem", "Can", "Leo", "Vir", "Lib", "Sco", "Sag", "Cap", "Aqu", "Pis"]
SIGN_KEYS = ["Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
             "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"]
SIGN_JA = ["牡羊座", "牡牛座", "双子座", "蟹座", "獅子座", "乙女座",
           "天秤座", "蠍座", "射手座", "山羊座", "水瓶座", "魚座"]
# 支配星（現代）。蠍・水瓶・魚は古典支配星も併記
RULER = ["Mars", "Venus", "Mercury", "Moon", "Sun", "Mercury",
         "Venus", "Pluto", "Jupiter", "Saturn", "Uranus", "Neptune"]
TRAD_RULER = {7: "Mars", 10: "Saturn", 11: "Jupiter"}
ANGULAR = {1, 4, 7, 10}
NO_RETRO = {"Sun", "Moon"}


def house_from(sign_idx: int, planet_sign: str) -> int:
    """ホールサイン（太陽星座＝1ハウス）で何ハウスか"""
    return (SIGNS.index(planet_sign) - sign_idx) % 12 + 1


def lon(p: dict) -> float:
    return SIGNS.index(p["sign"]) * 30 + p["degree"]


def _snap_index(snapshots):
    return {s["date"]: {p["name"]: p for p in s["planet_positions"]} for s in snapshots}


STATION_PLANETS = {"Mercury", "Venus", "Mars"}  # 留の日付を出すのはこの3つだけ
EPS = 0.02       # 小数2桁データの丸め誤差。これ未満の変化は「動いていない」とみなす
WINDOW = 3       # 逆行判定は前後3日の度数差で見る
WINDOW_SLOW = 7  # 木星より遠い天体は1日の移動が丸めに埋もれるため前後7日で見る
SLOW = {"Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"}


def motion_events(snapshots):
    """逆行状態・留・サイン移動を判定する。

    - 逆行: 前後WINDOW日（外惑星はWINDOW_SLOW日）の黄経差で判定（月初・月末は片側のみ）。差がEPS未満なら None（留付近）
    - 留: 水星・金星・火星のみ。前日比がEPSを超える日の向きが切り替わった日
      （外惑星は1日の移動が小数2桁の丸めに埋もれ、留の日付が数日ぶれるため出さない）
    - サイン移動: 月以外
    """
    idx = _snap_index(snapshots)
    dates = sorted(idx)
    retro = {}
    for j, d in enumerate(dates):
        for name in idx[d]:
            w = WINDOW_SLOW if name in SLOW else WINDOW
            a, b = dates[max(0, j - w)], dates[min(len(dates) - 1, j + w)]
            if name in NO_RETRO or name not in idx[a] or name not in idx[b]:
                continue
            diff = (lon(idx[b][name]) - lon(idx[a][name]) + 180) % 360 - 180
            retro[(d, name)] = None if abs(diff) < EPS else diff < 0

    stations, ingresses, prev_dir = [], [], {}
    for d0, d1 in zip(dates, dates[1:]):
        if date.fromisoformat(d1) - date.fromisoformat(d0) != timedelta(days=1):
            prev_dir.clear()
            continue
        for name, p1 in idx[d1].items():
            p0 = idx[d0].get(name)
            if not p0:
                continue
            if p0["sign"] != p1["sign"] and name != "Moon":
                ingresses.append({"planet": name, "date": d1, "from": p0["sign"], "to": p1["sign"]})
            if name not in STATION_PLANETS:
                continue
            diff = (lon(p1) - lon(p0) + 180) % 360 - 180
            if abs(diff) < EPS:
                continue
            is_r = diff < 0
            if name in prev_dir and prev_dir[name] != is_r:
                stations.append({"planet": name, "date": d0,
                                 "turn": "retrograde" if is_r else "direct",
                                 "sign": p0["sign"]})
            prev_dir[name] = is_r
    return retro, stations, ingresses


def build_zodiac_context(transits: dict, max_aspects: int = 12) -> dict:
    # 天体位置のない日（テスト用ローダーなど）は使わない。全天体が揃わなければ出さない
    snaps = [s for s in transits.get("daily_snapshots") or [] if s.get("planet_positions")]
    idx = _snap_index(snaps)
    dates = sorted(idx)
    if not dates or any(not set(RULER) <= set(idx[d]) for d in (dates[0], dates[-1])):
        return {}
    retro, stations, ingresses = motion_events(snaps)
    first, last = dates[0], dates[-1]

    # アスペクトの成立日のサインを daily_snapshots から引く
    aspects = []
    for a in transits.get("major_aspects", [])[:max_aspects]:
        pos = idx.get(a["date"], {})
        if a["planet_a"] not in pos or a["planet_b"] not in pos:
            continue
        aspects.append((a, pos[a["planet_a"]]["sign"], pos[a["planet_b"]]["sign"]))

    ctx = {}
    for i, key in enumerate(SIGN_KEYS):
        sign = SIGNS[i]
        # 月初・月末の天体配置（ハウス）
        def placements(d):
            return {n: {"sign": p["sign"], "house": house_from(i, p["sign"]),
                        "retrograde": retro.get((d, n), False)}
                    for n, p in idx[d].items() if n != "Moon"}

        asp_list = []
        for a, sa, sb in aspects:
            ha, hb = house_from(i, sa), house_from(i, sb)
            asp_list.append({
                "date": a["date"], "label": a["label"], "orb": a["orb"],
                "start_date": a.get("start_date"), "end_date": a.get("end_date"),
                "houses": [ha, hb],
                "angular": bool({ha, hb} & ANGULAR),
                # 月のアスペクトは1日で終わるため、主役候補からは外す
                "moon_aspect": "Moon" in (a["planet_a"], a["planet_b"]),
            })

        planets_in_sign = sorted({n for d in dates for n, p in idx[d].items()
                                  if p["sign"] == sign and n != "Moon"})
        moon_days = [d for d in dates if idx[d].get("Moon", {}).get("sign") == sign]

        ruler = RULER[i]
        rp_first, rp_last = idx[first][ruler], idx[last][ruler]
        ruler_info = {
            "planet": ruler,
            "month_start": {"sign": rp_first["sign"], "house": house_from(i, rp_first["sign"])},
            "month_end": {"sign": rp_last["sign"], "house": house_from(i, rp_last["sign"]),
                          "retrograde": retro.get((last, ruler), False)},
            "stations": [s for s in stations if s["planet"] == ruler],
        }
        if i in TRAD_RULER:
            t = TRAD_RULER[i]
            ruler_info["traditional"] = {"planet": t, "sign": idx[last][t]["sign"],
                                         "house": house_from(i, idx[last][t]["sign"])}

        ctx[key] = {
            "sign_ja": SIGN_JA[i],
            "aspects_by_house": asp_list,
            "angular_aspects": [x["label"] + f"（{x['date']}・{x['houses'][0]}–{x['houses'][1]}ハウス）"
                                for x in asp_list if x["angular"] and not x["moon_aspect"]],
            "planets_in_sign": planets_in_sign,
            "moon_in_sign_dates": moon_days,
            "ruler": ruler_info,
            "stations_by_house": [dict(s, house=house_from(i, s["sign"])) for s in stations],
            "ingresses_by_house": [dict(g, house_from=house_from(i, g["from"]),
                                        house_to=house_from(i, g["to"])) for g in ingresses],
            "placements_month_start": placements(first),
            "placements_month_end": placements(last),
        }
    return ctx
