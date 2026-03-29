
from __future__ import annotations

from typing import Any

HOUSE_MEANINGS = {
    1: "本人の出方・第一印象・身体感覚",
    2: "価値観・お金・言葉・安心感",
    3: "学び・発信・実務・兄弟姉妹・試行錯誤",
    4: "基盤・家庭・居場所・内面の安定",
    5: "創造性・恋愛・自己表現・喜び",
    6: "仕事・調整・改善・奉仕・課題処理",
    7: "対人関係・パートナー・契約",
    8: "深い結びつき・変化・共有・危機管理",
    9: "信念・学び・精神性・遠方・師",
    10: "社会的立場・仕事・評価・責任",
    11: "仲間・組織・ネットワーク・成果",
    12: "内面・休息・無意識・孤独・手放し",
}
SIGN_WORDS = {
    "Ari": "牡羊座", "Tau": "牡牛座", "Gem": "双子座", "Can": "蟹座", "Leo": "獅子座", "Vir": "乙女座",
    "Lib": "天秤座", "Sco": "蠍座", "Sag": "射手座", "Cap": "山羊座", "Aqu": "水瓶座", "Pis": "魚座",
    "Aries": "牡羊座", "Taurus": "牡牛座", "Gemini": "双子座", "Cancer": "蟹座", "Leo": "獅子座", "Virgo": "乙女座",
    "Libra": "天秤座", "Scorpio": "蠍座", "Sagittarius": "射手座", "Capricorn": "山羊座", "Aquarius": "水瓶座", "Pisces": "魚座",
}


def _as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _as_list(x: Any) -> list[Any]:
    return x if isinstance(x, list) else []


def _pick(root: dict[str, Any], *keys: str) -> Any:
    cur: Any = root
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _jp_sign(sign: Any) -> str:
    s = str(sign or "")
    return SIGN_WORDS.get(s, s)


def _planet_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pmap = payload.get("planets_map")
    if isinstance(pmap, dict):
        return {str(k): v for k, v in pmap.items() if isinstance(v, dict)}
    planets = payload.get("planets")
    if isinstance(planets, dict):
        return {str(k): v for k, v in planets.items() if isinstance(v, dict)}
    out: dict[str, dict[str, Any]] = {}
    if isinstance(planets, list):
        for p in planets:
            if isinstance(p, dict) and p.get("name"):
                out[str(p["name"])] = p
    return out


def _house_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    houses = payload.get("houses")
    if isinstance(houses, dict):
        return _as_list(houses.get("houses"))
    return _as_list(houses)


def _house_sign_lines(houses: list[dict[str, Any]], max_houses: int = 12) -> list[str]:
    lines: list[str] = []
    for h in houses[:max_houses]:
        no = h.get("house_no") or h.get("house")
        if not no:
            continue
        sign = h.get("sign") or h.get("rashi_name") or h.get("rashi")
        lines.append(f"{no}室: {_jp_sign(sign)} / {HOUSE_MEANINGS.get(int(no), '')}")
    return lines


def _find_planet(planets: dict[str, dict[str, Any]], name: str) -> dict[str, Any]:
    return _as_dict(planets.get(name))


def _western_house_readings(western: dict[str, Any]) -> list[str]:
    houses = _house_list(western)
    return _house_sign_lines(houses)


def _vedic_house_readings(vedic: dict[str, Any]) -> list[str]:
    houses = _house_list(vedic)
    return _house_sign_lines(houses)


def _western_themes(western: dict[str, Any]) -> list[dict[str, Any]]:
    planets = _planet_map(western)
    houses = _house_list(western)
    themes: list[dict[str, Any]] = []
    asc_sign = _jp_sign(_pick(_as_dict(western.get("angles")), "asc_sign") or (houses[0].get("sign") if houses else None))

    sun = _find_planet(planets, "Sun")
    mercury = _find_planet(planets, "Mercury")
    moon = _find_planet(planets, "Moon")
    venus = _find_planet(planets, "Venus")

    sun_house = sun.get("house_no") or sun.get("house")
    sun_sign = _jp_sign(sun.get("sign"))
    mercury_house = mercury.get("house_no") or mercury.get("house")
    mercury_sign = _jp_sign(mercury.get("sign"))
    moon_house = moon.get("house_no") or moon.get("house")
    moon_sign = _jp_sign(moon.get("sign"))
    venus_house = venus.get("house_no") or venus.get("house")

    # Leadership style / misread
    if sun_house == 10 and sun_sign == "獅子座":
        if asc_sign == "蠍座" or (mercury_house == 11 and mercury_sign == "乙女座"):
            themes.append({
                "id": "leadership_style",
                "label": "リーダーシップの出方",
                "summary": "前に立って旗を振るより、裏で流れを決める軍師型・実権型に近い",
                "client_text": "いわゆる“目立つリーダー”というより、裏で全体の流れを読んで動かすタイプです。",
                "reader_explanation": "表の顔役というより、情報・判断・実務を握って場を支配するタイプ。相談者が“リーダーっぽくない”と感じやすいが、実際はかなり場を動かしている。",
                "reasons": [
                    f"{asc_sign}ASCは手の内を全部見せず、本質や力関係を読む傾向が強い。",
                    f"{mercury_sign}の水星が{mercury_house}室にあるため、仲間・組織の中で実務整理や情報管理に強い。" if mercury_house else "水星が強く、実務整理や情報管理に優れる。",
                    "10室獅子座の太陽は、社会的な場で“この人が認めるかどうか”が基準になりやすい。",
                ],
                "evidence": [
                    f"ASC {_jp_sign(asc_sign)}" if asc_sign else "ASC",
                    f"Mercury {mercury_sign} {mercury_house}H" if mercury_house else f"Mercury {mercury_sign}",
                    f"Sun {sun_sign} {sun_house}H",
                ],
                "misread_risk": True,
                "misread_note": "“前に出るリーダー”像とズレるため、本人に違和感が出やすいテーマ。",
            })
        else:
            themes.append({
                "id": "leadership_style",
                "label": "リーダーシップの出方",
                "summary": "社会的な場で存在感を出しやすく、責任のある立場を担いやすい",
                "client_text": "人前に出る役割や、判断を求められる立場に自然と押し上げられやすいです。",
                "reader_explanation": "10室太陽が社会的可視性を強める。",
                "reasons": ["10室の太陽は社会的役割・評価・責任への集中を示す。"],
                "evidence": [f"Sun {sun_sign} {sun_house}H"],
                "misread_risk": False,
            })

    # Work style
    if mercury_house in (6, 10, 11) or mercury_sign == "乙女座":
        reasons = []
        if mercury_sign:
            reasons.append(f"{mercury_sign}の水星は、言葉・整理・分析・改善に強い。")
        if mercury_house:
            reasons.append(f"水星が{mercury_house}室にあり、{HOUSE_MEANINGS.get(int(mercury_house), '')}へ知性が向きやすい。")
        themes.append({
            "id": "work_style",
            "label": "仕事の進め方",
            "summary": "勢いで押すより、問題点を見つけて整理・修正しながら進めるタイプ",
            "client_text": "感覚だけで進むより、段取りや改善を入れながら形にする方が力を発揮しやすいです。",
            "reader_explanation": "実務整理・運用改善・情報ハンドリングに強みが出やすい。",
            "reasons": reasons,
            "evidence": [f"Mercury {mercury_sign} {mercury_house}H" if mercury_house else f"Mercury {mercury_sign}"],
            "misread_risk": mercury_sign == "乙女座",
            "misread_note": "“細かい人”で終わらせると浅くなる。実際は、全体を成立させるために細部を整えるタイプ。" if mercury_sign == "乙女座" else "",
        })

    # Emotional pattern
    if moon_sign or moon_house:
        reasons = []
        if moon_sign:
            reasons.append(f"月が{moon_sign}にあり、感情の満足はその星座のテーマに沿って動く。")
        if moon_house:
            reasons.append(f"月が{moon_house}室にあり、感情は{HOUSE_MEANINGS.get(int(moon_house), '')}に影響されやすい。")
        themes.append({
            "id": "emotional_pattern",
            "label": "感情の扱い方",
            "summary": "感情は理念や意味づけと結びつけて処理しやすく、納得がないと動きにくい" if moon_sign == "射手座" else "感情の扱い方には、気分だけでなく意味づけや納得が必要になりやすい",
            "client_text": "気持ちだけで動くより、『これに意味があるか』を確認してから進みやすいです。",
            "reader_explanation": "月のサインとハウスから、感情の満足条件を説明する。",
            "reasons": reasons,
            "evidence": [f"Moon {moon_sign} {moon_house}H" if moon_house else f"Moon {moon_sign}"],
            "misread_risk": False,
        })

    if venus_house in (7, 10) and venus.get("retrograde"):
        themes.append({
            "id": "relationship_style",
            "label": "対人・親密さの出方",
            "summary": "人と関わる力はあるが、見た目より慎重で、自分の基準を通して相手を見やすい",
            "client_text": "対人は得意でも、誰にでも同じ温度で近づくわけではなく、かなり基準を見ています。",
            "reader_explanation": "金星逆行は、好みや対人感覚が外向きに均一ではなく、内的基準を強めやすい。",
            "reasons": [
                f"金星が{venus_house}室にあり、対人や社会的関係のテーマが前に出やすい。",
                "ただし逆行しているため、好みや評価軸はかなり内的で、選別が入る。",
            ],
            "evidence": [f"Venus {_jp_sign(venus.get('sign'))} {venus_house}H retrograde"],
            "misread_risk": True,
            "misread_note": "“感じがいい＝誰にでも開く”ではない。",
        })

    return themes


def _vedic_themes(vedic: dict[str, Any], vedic_structure: dict[str, Any], vedic_flags: list[dict[str, Any]]) -> list[dict[str, Any]]:
    themes: list[dict[str, Any]] = []
    hlp = _as_dict(vedic_structure.get("house_lords_placement"))
    yogas = _as_list(vedic_structure.get("yogas"))
    dasha = _as_dict(vedic_structure.get("dasha"))

    lord7 = _as_dict(hlp.get("7") or hlp.get(7))
    lord10 = _as_dict(hlp.get("10") or hlp.get(10))
    lord11 = _as_dict(hlp.get("11") or hlp.get(11))
    lord5 = _as_dict(hlp.get("5") or hlp.get(5))

    if lord10:
        themes.append({
            "id": "vedic_public_role",
            "label": "社会的な立ち位置（インド占星術）",
            "summary": f"10室支配星が{lord10.get('placed_in_house')}室にあり、仕事の力は{HOUSE_MEANINGS.get(int(lord10.get('placed_in_house') or 0), 'その領域')}を通して現れやすい",
            "client_text": f"仕事運は、{HOUSE_MEANINGS.get(int(lord10.get('placed_in_house') or 0), 'その領域')}を通じて育ちやすいです。",
            "reader_explanation": "10室支配星の在住先を、職業テーマの“出口”として説明する。",
            "reasons": [
                f"10室支配星は {lord10.get('lord')}。",
                f"その {lord10.get('lord')} が {lord10.get('placed_in_house')}室・{_jp_sign(lord10.get('placed_in_rashi'))} にあるため。",
                f"強さスコアは {lord10.get('strength_score')}。",
            ],
            "evidence": [f"10L {lord10.get('lord')} -> {lord10.get('placed_in_house')}H / {_jp_sign(lord10.get('placed_in_rashi'))}"],
            "misread_risk": False,
        })

    if lord7:
        summary = f"対人やパートナーシップは、{HOUSE_MEANINGS.get(int(lord7.get('placed_in_house') or 0), 'その領域')}の文脈で現れやすい"
        misread = False
        note = ""
        if int(lord7.get('placed_in_house') or 0) == 6:
            summary = "対人やパートナーシップが、癒やしより調整・課題処理として現れやすい"
            misread = True
            note = "“人が嫌い”ではなく、関係を実務や責任で回しやすい。"
        elif int(lord7.get('placed_in_house') or 0) == 10:
            summary = "パートナーシップや対人は、社会的な役割・評価・仕事と結びつきやすい"
        themes.append({
            "id": "vedic_relationship_style",
            "label": "対人関係の出方（インド占星術）",
            "summary": summary,
            "client_text": summary,
            "reader_explanation": "7室支配星の在住ハウスから、相手との関わり方の文脈を説明する。",
            "reasons": [
                f"7室支配星は {lord7.get('lord')}。",
                f"その {lord7.get('lord')} が {lord7.get('placed_in_house')}室・{_jp_sign(lord7.get('placed_in_rashi'))} にあるため。",
            ],
            "evidence": [f"7L {lord7.get('lord')} -> {lord7.get('placed_in_house')}H / {_jp_sign(lord7.get('placed_in_rashi'))}"],
            "misread_risk": misread,
            "misread_note": note,
        })

    if lord11:
        themes.append({
            "id": "vedic_network_style",
            "label": "仲間・組織の中での役割（インド占星術）",
            "summary": f"11室支配星が{lord11.get('placed_in_house')}室にあり、仲間・組織との関わりは{HOUSE_MEANINGS.get(int(lord11.get('placed_in_house') or 0), 'その領域')}と結びつきやすい",
            "client_text": f"仲間や組織とのつながりは、{HOUSE_MEANINGS.get(int(lord11.get('placed_in_house') or 0), 'その領域')}を通して強まりやすいです。",
            "reader_explanation": "11室支配星は、ネットワークと成果の出し方を示す。",
            "reasons": [
                f"11室支配星は {lord11.get('lord')}。",
                f"その {lord11.get('lord')} が {lord11.get('placed_in_house')}室・{_jp_sign(lord11.get('placed_in_rashi'))} にあるため。",
            ],
            "evidence": [f"11L {lord11.get('lord')} -> {lord11.get('placed_in_house')}H / {_jp_sign(lord11.get('placed_in_rashi'))}"],
            "misread_risk": False,
        })

    if lord5:
        themes.append({
            "id": "vedic_creativity_style",
            "label": "喜び・創造性の出方（インド占星術）",
            "summary": f"5室支配星が{lord5.get('placed_in_house')}室にあり、喜びや創造性は{HOUSE_MEANINGS.get(int(lord5.get('placed_in_house') or 0), 'その領域')}を通して出やすい",
            "client_text": f"好きなことや自己表現は、{HOUSE_MEANINGS.get(int(lord5.get('placed_in_house') or 0), 'その領域')}を通じて育ちやすいです。",
            "reader_explanation": "5室支配星の在住先から、恋愛・創造性・表現の出口を読む。",
            "reasons": [
                f"5室支配星は {lord5.get('lord')}。",
                f"その {lord5.get('lord')} が {lord5.get('placed_in_house')}室・{_jp_sign(lord5.get('placed_in_rashi'))} にあるため。",
            ],
            "evidence": [f"5L {lord5.get('lord')} -> {lord5.get('placed_in_house')}H / {_jp_sign(lord5.get('placed_in_rashi'))}"],
            "misread_risk": int(lord5.get('placed_in_house') or 0) == 12,
            "misread_note": "表現しないのではなく、まず内面で熟成させやすい。" if int(lord5.get('placed_in_house') or 0) == 12 else "",
        })

    yoga_names = [str(y.get("name")) for y in yogas if isinstance(y, dict) and y.get("name")]
    if yoga_names:
        themes.append({
            "id": "vedic_yoga_context",
            "label": "ヨーガの読み方（インド占星術）",
            "summary": "良い配置と注意配置が混在しており、“単純に良い・悪い”ではなく、使い方で差が出る命式",
            "client_text": "運の良さはありますが、自然に放っておいて全部うまくいくというより、使い方次第で開くタイプです。",
            "reader_explanation": "ヨーガは“保証”ではなく、どの方向へ力が集まりやすいかの補助線として使う。",
            "reasons": [f"検出ヨーガ: {', '.join(yoga_names)}"],
            "evidence": yoga_names,
            "misread_risk": "Kemadruma Yoga" in yoga_names,
            "misread_note": "Kemadruma があっても孤立が運命的に固定されるわけではない。" if "Kemadruma Yoga" in yoga_names else "",
        })

    if dasha:
        maha = _pick(dasha, "maha", "lord")
        antara = _pick(dasha, "antara", "lord")
        active = _as_list(dasha.get("active_themes"))
        themes.append({
            "id": "vedic_timing",
            "label": "現在運のテーマ（インド占星術）",
            "summary": f"現在は {maha or '-'}期 / {antara or '-'}期で、{('・'.join(map(str, active[:3]))) if active else '現在運のテーマ'} が表に出やすい",
            "client_text": f"今は {maha or '-'}期 / {antara or '-'}期で、今の課題や役割がはっきり前に出やすい時期です。",
            "reader_explanation": "時期運は“吉凶”ではなく、“何が前面化するか”として説明する。",
            "reasons": [
                f"Mahadasha: {maha or '-'}",
                f"Antardasha: {antara or '-'}",
                f"Active themes: {', '.join(map(str, active[:5])) if active else '-'}",
            ],
            "evidence": [f"{maha or '-'} / {antara or '-'}"],
            "misread_risk": False,
        })

    # add vedic flags as extra reasons/cards if not already covered
    for flag in vedic_flags:
        if not isinstance(flag, dict):
            continue
        fid = str(flag.get("id") or "")
        if fid in {"pressure_hardening", "inner_isolation_tendency"}:
            themes.append({
                "id": fid,
                "label": "注意して読むポイント（インド占星術）",
                "summary": str(flag.get("manifestation") or ""),
                "client_text": str(flag.get("manifestation") or ""),
                "reader_explanation": "フラグは脅しではなく、出やすい偏りとして読む。",
                "reasons": [str(flag.get("countermeasures") or "")],
                "evidence": [jsonable(flag.get("evidence"))],
                "misread_risk": False,
            })
    return themes


def jsonable(x: Any) -> str:
    if isinstance(x, dict):
        return ", ".join(f"{k}={v}" for k, v in x.items())
    if isinstance(x, list):
        return ", ".join(map(str, x))
    return str(x)


def _shichu_themes(shichu: dict[str, Any]) -> list[dict[str, Any]]:
    themes: list[dict[str, Any]] = []
    sr = _as_dict(shichu.get("structure_report"))
    normalized = _as_dict(shichu.get("normalized_data"))
    label = sr.get("label") or _pick(sr, "strength_index", "label") or sr.get("strength_label")
    feats = _as_list(sr.get("dominant_features"))
    day_master = sr.get("day_master") or _pick(normalized, "ten_gods", "day_master")
    seasonal_context = _as_dict(sr.get("seasonal_context"))

    if label or feats or day_master:
        reasons: list[str] = []
        if day_master:
            reasons.append(f"日主は「{day_master}」で、性質判断はこの中心軸を土台に見ていきます。")
        if label:
            reasons.append(f"全体の強弱判定は「{label}」で、極端すぎないが偏りは拾う前提です。")
        for feat in feats[:4]:
            s = str(feat)
            if "相対的に多い" in s:
                reasons.append(f"{s}ため、その要素の性質は前に出やすいです。")
            elif "相対的に少ない" in s:
                reasons.append(f"{s}ため、その要素は意識して補うとバランスを取りやすいです。")
            elif s.startswith("月支は"):
                reasons.append(f"{s}ので、季節や環境条件の影響を受けながら性質が表に出ます。")
            elif s.startswith("大運方向は"):
                reasons.append(f"{s}で、時期の流れ方にも一定の傾向があります。")
            else:
                reasons.append(s)
        relation = seasonal_context.get("relation_to_day_master")
        month_branch = seasonal_context.get("month_branch")
        month_element = seasonal_context.get("month_element")
        if relation and month_branch and month_element:
            if relation == "controlled_by_month":
                reasons.append(f"月支は「{month_branch}（{month_element}）」で、日主に対しては抑えや現実条件として働きやすい配置です。")
            elif relation == "supported_by_month":
                reasons.append(f"月支は「{month_branch}（{month_element}）」で、日主を支えやすい季節感があります。")

        summary_bits: list[str] = []
        if day_master:
            summary_bits.append(f"日主は「{day_master}」")
        if label:
            summary_bits.append(f"全体傾向は「{label}」")
        if feats:
            summary_bits.append("特徴は「" + " / ".join(map(str, feats[:3])) + "」")
        summary_text = "、".join(summary_bits) if summary_bits else "命式バランスを整理しました"

        themes.append({
            "id": "shichu_balance",
            "label": "命式バランス（四柱推命）",
            "summary": summary_text,
            "client_text": f"生まれ持った土台は『{label or 'バランスあり'}』で、強い要素と不足気味の要素の両方を見ていきます。",
            "reader_explanation": "強弱や五行の偏りを、そのままラベル化せず、実際の出方と補い方に翻訳して使います。",
            "reasons": reasons,
            "evidence": [x for x in [label or "", *list(map(str, feats[:5]))] if x],
            "misread_risk": False,
        })

    daiun = _pick(normalized, "daiun")
    if isinstance(daiun, dict) and daiun:
        reasons: list[str] = []
        direction = daiun.get("direction")
        start_age_text = daiun.get("start_age_text")
        if direction == "forward":
            reasons.append("大運は順行で進み、年齢とともに流れが前へ展開していく見方を取ります。")
        elif direction == "backward":
            reasons.append("大運は逆行で進み、通常とは逆順でテーマが巡っていく見方を取ります。")
        if start_age_text:
            reasons.append(f"大運の切り替わりはおおよそ {start_age_text} 頃から始まる目安です。")
        items = _as_list(daiun.get("items"))
        if items:
            current = items[0]
            if isinstance(current, dict) and current.get("kanshi"):
                reasons.append(f"現在の主な流れは「{current.get('kanshi')}」で、時期による濃淡も読みの補助線になります。")
        themes.append({
            "id": "shichu_timing",
            "label": "運の流れ（四柱推命）",
            "summary": "大運は固定の性格ではなく、今どの要素が強まりやすいかを見る補助線です。",
            "client_text": "今の流れは、生まれ持った性質に加えて“時期の後押し”や強調点も受けています。",
            "reader_explanation": "大運は吉凶の断定ではなく、何が前面化しやすいかを整理するために使います。",
            "reasons": reasons,
            "evidence": [x for x in [daiun.get("direction"), daiun.get("start_age_text")] if x],
            "misread_risk": False,
        })
    return themes


def _detect_structure_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return summary if isinstance(summary, dict) else {}


def _detect_western(raw: dict[str, Any], structure_summary: dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw.get("western"), dict):
        return raw["western"]
    if raw.get("module") == "western":
        return raw
    # fallback to structure summary snapshot
    if structure_summary.get("planets") and structure_summary.get("houses"):
        return {
            "planets": structure_summary.get("planets"),
            "houses": structure_summary.get("houses"),
            "_derived": _as_dict(structure_summary.get("_derived")),
        }
    return {}


def _detect_vedic(raw: dict[str, Any], structure_summary: dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw.get("vedic"), dict):
        return raw["vedic"]
    if raw.get("module") == "vedic" or raw.get("house_lords") or raw.get("dasha"):
        return raw
    # structure summary carries vedic snapshot in this project
    if structure_summary.get("_derived") or structure_summary.get("planets"):
        return {
            "planets": structure_summary.get("planets"),
            "houses": structure_summary.get("houses"),
            "_derived": _as_dict(structure_summary.get("_derived")),
        }
    return {}


def _detect_shichu(raw: dict[str, Any], structure_summary: dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw.get("shichusuimei"), dict):
        return raw["shichusuimei"]
    if raw.get("module") == "shichusuimei":
        return raw
    return _as_dict(raw.get("shichu") or structure_summary.get("shichu"))


def _western_flags(source: dict[str, Any]) -> list[dict[str, Any]]:
    derived = _as_dict(source.get("_derived"))
    flags = derived.get("risk_flags") or source.get("risk_flags") or []
    return [f for f in _as_list(flags) if isinstance(f, dict)]


def _vedic_structure(source: dict[str, Any]) -> dict[str, Any]:
    derived = _as_dict(source.get("_derived"))
    return _as_dict(derived.get("vedic_structure") or source.get("vedic_structure") or {})


def _vedic_flags(source: dict[str, Any]) -> list[dict[str, Any]]:
    derived = _as_dict(source.get("_derived"))
    flags = derived.get("vedic_flags") or source.get("vedic_flags") or []
    return [f for f in _as_list(flags) if isinstance(f, dict)]


def _combined_headline(western_themes: list[dict[str, Any]], vedic_themes: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    if western_themes:
        parts.append(western_themes[0].get("summary", ""))
    if vedic_themes:
        parts.append(vedic_themes[0].get("summary", ""))
    parts = [p for p in parts if p]
    return " / ".join(parts[:2]) or "複数占術を横断した読み筋を表示"


def _ai_judgment(western_themes: list[dict[str, Any]], vedic_themes: list[dict[str, Any]], shichu_themes: list[dict[str, Any]]) -> str:
    lines = ["総合判断（AI補助）", ""]
    if western_themes:
        lines.append(f"- 西洋占星術では『{western_themes[0].get('summary', '')}』という出方が強く見えます。")
    if vedic_themes:
        lines.append(f"- インド占星術では『{vedic_themes[0].get('summary', '')}』として裏づけが入ります。")
    if shichu_themes:
        lines.append(f"- 四柱推命では『{shichu_themes[0].get('summary', '')}』として、土台のバランスを補足できます。")
    lines.append("")
    lines.append("占い師向けメモ: 相談者が言葉に違和感を示したら、“ラベルそのもの”ではなく“どう現れるか”を説明するのが有効です。")
    return "\n".join(lines).strip()


def build_full_astrologer_summary(raw_result: dict[str, Any], structure_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = _as_dict(raw_result)
    ss = _detect_structure_summary(structure_summary or {})

    western = _detect_western(raw, ss)
    vedic = _detect_vedic(raw, ss)
    shichu = _detect_shichu(raw, ss)

    western_themes = _western_themes(western) if western else []
    western_flags = _western_flags(western) if western else []
    western_house_reads = _western_house_readings(western) if western else []

    vs = _vedic_structure(vedic) if vedic else {}
    vedic_flags = _vedic_flags(vedic) if vedic else []
    vedic_themes = _vedic_themes(vedic, vs, vedic_flags) if vedic else []
    vedic_house_reads = _vedic_house_readings(vs if vs else vedic) if vedic else []

    shichu_themes = _shichu_themes(shichu) if shichu else []

    return {
        "combined": {
            "headline": _combined_headline(western_themes, vedic_themes),
            "ai_judgment": _ai_judgment(western_themes, vedic_themes, shichu_themes),
        },
        "western": {
            "available": bool(western),
            "themes": western_themes,
            "house_readings": western_house_reads,
            "flags": western_flags,
        },
        "vedic": {
            "available": bool(vedic),
            "themes": vedic_themes,
            "house_readings": vedic_house_reads,
            "flags": vedic_flags,
            "vedic_structure": vs,
        },
        "shichu": {
            "available": bool(shichu),
            "themes": shichu_themes,
        },
        "structure_summary": ss,
        "raw": raw,
    }

def _render_theme_block(theme: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    label = str(theme.get("label") or "テーマ")
    summary = str(theme.get("summary") or "").strip()
    client = str(theme.get("client_text") or "").strip()
    reader = str(theme.get("reader_explanation") or "").strip()
    reasons = [str(r).strip() for r in _as_list(theme.get("reasons")) if str(r).strip()]
    evidence = [str(e).strip() for e in _as_list(theme.get("evidence")) if str(e).strip()]
    lines.append(f"【{label}】")
    if summary:
        lines.append(summary)
    if client:
        lines.append(f"相談者向けの言い方: {client}")
    if reader:
        lines.append(f"占い師向け説明: {reader}")
    if reasons:
        lines.append("理由:")
        lines.extend([f"- {r}" for r in reasons])
    if evidence:
        lines.append("根拠:")
        lines.extend([f"- {e}" for e in evidence])
    note = str(theme.get("misread_note") or "").strip()
    if theme.get("misread_risk") and note:
        lines.append(f"違和感が出やすい点: {note}")
    return lines


def render_astrologer_memo(summary: dict[str, Any]) -> str:
    s = _as_dict(summary)
    out: list[str] = []
    headline = _pick(s, "combined", "headline")
    judgment = _pick(s, "combined", "ai_judgment")
    if headline:
        out.append(f"総合ヘッドライン: {headline}")
        out.append("")
    if judgment:
        out.append("【総合判断（AI補助）】")
        out.append(str(judgment).strip())
        out.append("")

    sections = [
        ("西洋占星術", _pick(s, "western", "themes")),
        ("インド占星術", _pick(s, "vedic", "themes")),
        ("四柱推命", _pick(s, "shichu", "themes")),
    ]
    for title, themes in sections:
        theme_list = [t for t in _as_list(themes) if isinstance(t, dict)]
        if not theme_list:
            continue
        out.append(f"■ {title}")
        out.append("")
        for idx, theme in enumerate(theme_list):
            out.extend(_render_theme_block(theme))
            if idx != len(theme_list) - 1:
                out.append("")
        out.append("")

    return "\n".join(line for line in out).strip()
