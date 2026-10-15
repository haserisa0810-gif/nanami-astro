from __future__ import annotations

"""四柱推命の命式内相互作用と神殺（フェーズ1）。

このモジュールは組み合わせの計算事実だけを返し、吉凶や自然言語の
解釈は行わない。柱名は大運・流年などへの拡張を妨げない任意文字列とする。
"""

from itertools import combinations
from typing import Iterable, Sequence

NamedSymbol = tuple[str, str]

_PILLAR_ORDER = {"year": 0, "month": 1, "day": 2, "hour": 3, "daiun": 4, "ryunen": 5}

_STEM_COMBINATIONS = {
    frozenset(("甲", "己")): "土",
    frozenset(("乙", "庚")): "金",
    frozenset(("丙", "辛")): "水",
    frozenset(("丁", "壬")): "木",
    frozenset(("戊", "癸")): "火",
}
_BRANCH_HARMONIES = {
    frozenset(pair) for pair in (("子", "丑"), ("寅", "亥"), ("卯", "戌"), ("辰", "酉"), ("巳", "申"), ("午", "未"))
}
_BRANCH_CLASHES = {
    frozenset(pair) for pair in (("子", "午"), ("丑", "未"), ("寅", "申"), ("卯", "酉"), ("辰", "戌"), ("巳", "亥"))
}
_TRINES = (
    (("申", "子", "辰"), "水"),
    (("寅", "午", "戌"), "火"),
    (("巳", "酉", "丑"), "金"),
    (("亥", "卯", "未"), "木"),
)
_DIRECTIONALS = (
    (("寅", "卯", "辰"), "木"),
    (("巳", "午", "未"), "火"),
    (("申", "酉", "戌"), "金"),
    (("亥", "子", "丑"), "水"),
)
_PUNISHMENT_GROUPS = (
    frozenset(("寅", "巳", "申")),
    frozenset(("丑", "戌", "未")),
)
_PUNISHMENT_PAIRS = {
    frozenset(pair)
    for pair in (("寅", "巳"), ("巳", "申"), ("申", "寅"), ("丑", "戌"), ("戌", "未"), ("未", "丑"), ("子", "卯"))
}
_SELF_PUNISHMENT = {"辰", "午", "酉", "亥"}
_HARMS = {
    frozenset(pair) for pair in (("子", "未"), ("丑", "午"), ("寅", "巳"), ("卯", "辰"), ("申", "亥"), ("酉", "戌"))
}
_DESTRUCTIONS = {
    frozenset(pair) for pair in (("子", "酉"), ("丑", "辰"), ("寅", "亥"), ("卯", "午"), ("巳", "申"), ("戌", "未"))
}
_STEM_CLASHES = {
    frozenset(pair) for pair in (("甲", "庚"), ("乙", "辛"), ("丙", "壬"), ("丁", "癸"))
}

_TENOTSU_KIJIN = {
    "甲": ("丑", "未"), "戊": ("丑", "未"), "庚": ("丑", "未"),
    "乙": ("子", "申"), "己": ("子", "申"),
    "丙": ("亥", "酉"), "丁": ("亥", "酉"),
    "壬": ("巳", "卯"), "癸": ("巳", "卯"),
    "辛": ("午", "寅"),
}
_EKIBA = {"申": "寅", "子": "寅", "辰": "寅", "寅": "申", "午": "申", "戌": "申", "巳": "亥", "酉": "亥", "丑": "亥", "亥": "巳", "卯": "巳", "未": "巳"}
_KANCHI = {"申": "酉", "子": "酉", "辰": "酉", "寅": "卯", "午": "卯", "戌": "卯", "巳": "午", "酉": "午", "丑": "午", "亥": "子", "卯": "子", "未": "子"}
_KAGAI = {"申": "辰", "子": "辰", "辰": "辰", "寅": "戌", "午": "戌", "戌": "戌", "巳": "丑", "酉": "丑", "丑": "丑", "亥": "未", "卯": "未", "未": "未"}
_YOUNJIN = {"甲": "卯", "丙": "午", "戊": "午", "庚": "酉", "壬": "子"}
_KAIGOU = {"庚辰", "庚戌", "壬辰", "戊戌"}


def _normalize(values: Iterable[NamedSymbol]) -> list[NamedSymbol]:
    return [(str(pillar), str(symbol)) for pillar, symbol in values]


def _sort_entries(entries: Sequence[NamedSymbol], input_order: dict[str, int]) -> list[NamedSymbol]:
    return sorted(entries, key=lambda item: (_PILLAR_ORDER.get(item[0], 100 + input_order[item[0]]), input_order[item[0]]))


def _interaction(
    *, rule_id: str, type_: str, category: str, entries: Sequence[NamedSymbol],
    input_order: dict[str, int], hour_known: bool, tags: list[str], detail: dict | None = None,
) -> dict:
    ordered = _sort_entries(entries, input_order)
    pillars = [pillar for pillar, _ in ordered]
    return {
        "rule_id": rule_id,
        "type": type_,
        "category": category,
        "scope": "natal",
        "pillars": pillars,
        "members": [symbol for _, symbol in ordered],
        "status": "detected",
        "provisional": not hour_known and "hour" in pillars,
        "tags": tags,
        "detail": detail or {},
    }


def detect_interactions(
    stems: Iterable[NamedSymbol], branches: Iterable[NamedSymbol], *, hour_known: bool = True,
) -> list[dict]:
    """干と支の名前付きリストから命式内の相互作用を検出する。"""
    stem_entries = _normalize(stems)
    branch_entries = _normalize(branches)
    input_order: dict[str, int] = {}
    for pillar, _ in (*stem_entries, *branch_entries):
        input_order.setdefault(pillar, len(input_order))
    items: list[dict] = []

    for pair in combinations(stem_entries, 2):
        element = _STEM_COMBINATIONS.get(frozenset((pair[0][1], pair[1][1])))
        if element:
            items.append(_interaction(
                rule_id="stem_combination_v1", type_="干合", category="stem", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["bonding", "attraction"],
                detail={"element": element, "transformation": "not_evaluated"},
            ))

    for pair in combinations(branch_entries, 2):
        symbols = frozenset((pair[0][1], pair[1][1]))
        if symbols in _BRANCH_HARMONIES:
            items.append(_interaction(
                rule_id="branch_harmony_v1", type_="支合", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["bonding", "stability"],
                detail={"transformation": "not_evaluated"},
            ))
        if symbols in _BRANCH_CLASHES:
            items.append(_interaction(
                rule_id="branch_clash_v1", type_="冲", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["tension", "change"],
            ))
        if symbols in _PUNISHMENT_PAIRS:
            complete = any(group.issubset({symbol for _, symbol in branch_entries}) and symbols.issubset(group) for group in _PUNISHMENT_GROUPS)
            detail = {"complete": True} if complete else {}
            items.append(_interaction(
                rule_id="branch_punishment_v1", type_="刑", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["friction", "self_undermining"], detail=detail,
            ))
        if pair[0][1] == pair[1][1] and pair[0][1] in _SELF_PUNISHMENT:
            items.append(_interaction(
                rule_id="branch_self_punishment_v1", type_="自刑", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["friction", "self_undermining"],
            ))
        if symbols in _HARMS:
            items.append(_interaction(
                rule_id="branch_harm_v1", type_="害", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["undermining", "interpersonal_strain"],
            ))
        if symbols in _DESTRUCTIONS:
            items.append(_interaction(
                rule_id="branch_destruction_v1", type_="破", category="branch", entries=pair,
                input_order=input_order, hour_known=hour_known, tags=["disruption"],
            ))

    for group, element in _TRINES:
        group_set = frozenset(group)
        matches = [entry for entry in branch_entries if entry[1] in group_set]
        if group_set.issubset({symbol for _, symbol in matches}):
            chosen = [next(entry for entry in matches if entry[1] == symbol) for symbol in group]
            items.append(_interaction(
                rule_id="branch_trine_v1", type_="三合", category="branch", entries=chosen,
                input_order=input_order, hour_known=hour_known, tags=["alliance", "amplification"],
                detail={"element": element, "transformation": "not_evaluated"},
            ))
        else:
            life, peak, grave = group
            valid_pairs = {frozenset((life, peak)), frozenset((peak, grave))}
            for pair in combinations(matches, 2):
                if frozenset((pair[0][1], pair[1][1])) in valid_pairs:
                    items.append(_interaction(
                        rule_id="branch_half_trine_v1", type_="半会", category="branch", entries=pair,
                        input_order=input_order, hour_known=hour_known, tags=["alliance", "amplification"],
                        detail={"element": element, "transformation": "not_evaluated"},
                    ))

    for group, element in _DIRECTIONALS:
        group_set = frozenset(group)
        if group_set.issubset({symbol for _, symbol in branch_entries}):
            chosen = [next(entry for entry in branch_entries if entry[1] == symbol) for symbol in group]
            items.append(_interaction(
                rule_id="branch_directional_v1", type_="方合", category="branch", entries=chosen,
                input_order=input_order, hour_known=hour_known, tags=["alliance", "seasonal_force"],
                detail={"element": element, "transformation": "not_evaluated"},
            ))
    return items


def detect_three_punishments(branches: Iterable[NamedSymbol], *, hour_known: bool = True) -> list[dict]:
    """三支が揃う刑を、成立に使った三つの柱を保持して返す。"""
    entries = _normalize(branches)
    input_order = {pillar: i for i, (pillar, _) in enumerate(entries)}
    return [
        _interaction(
            rule_id="branch_three_punishments_v1", type_="三刑", category="branch",
            entries=group, input_order=input_order, hour_known=hour_known,
            tags=["friction", "self_undermining"], detail={"complete": True},
        )
        for group in combinations(entries, 3)
        if frozenset(symbol for _, symbol in group) in _PUNISHMENT_GROUPS
    ]


def _shinsatsu_item(
    name: str, rule_id: str, basis: str, matches: Sequence[NamedSymbol], *, hour_known: bool, tags: list[str],
) -> dict:
    return {
        "name": name,
        "rule_id": rule_id,
        "basis": basis,
        "found_in": [pillar for pillar, _ in matches],
        "members": [symbol for _, symbol in matches],
        "provisional": not hour_known and any(pillar == "hour" for pillar, _ in matches),
        "tags": tags,
    }


def detect_shinsatsu(
    stems: Iterable[NamedSymbol], branches: Iterable[NamedSymbol], *, hour_known: bool = True,
) -> list[dict]:
    """指定された6種の神殺を検出する。"""
    stem_entries = _normalize(stems)
    branch_entries = _normalize(branches)
    input_order: dict[str, int] = {}
    for pillar, _ in (*stem_entries, *branch_entries):
        input_order.setdefault(pillar, len(input_order))
    stem_by_pillar = dict(stem_entries)
    branch_by_pillar = dict(branch_entries)
    day_stem = stem_by_pillar.get("day")
    day_branch = branch_by_pillar.get("day")
    items: list[dict] = []

    def append_branch_rule(name: str, rule_id: str, basis: str, targets: Iterable[str] | None, tags: list[str]) -> None:
        if not targets:
            return
        target_set = set(targets)
        matches = _sort_entries(
            [(pillar, branch) for pillar, branch in branch_entries if branch in target_set],
            input_order,
        )
        if matches:
            items.append(_shinsatsu_item(name, rule_id, basis, matches, hour_known=hour_known, tags=tags))

    append_branch_rule("天乙貴人", "tenotsu_kijin_v1", "day_stem", _TENOTSU_KIJIN.get(day_stem or ""), ["support", "protection"])
    append_branch_rule("駅馬", "ekiba_v1", "day_branch", [_EKIBA[day_branch]] if day_branch in _EKIBA else None, ["movement", "travel", "restlessness"])
    append_branch_rule("咸池", "kanchi_v1", "day_branch", [_KANCHI[day_branch]] if day_branch in _KANCHI else None, ["charm", "attraction"])
    append_branch_rule("華蓋", "kagai_v1", "day_branch", [_KAGAI[day_branch]] if day_branch in _KAGAI else None, ["solitude", "spirituality", "artistry"])
    append_branch_rule("羊刃", "youjin_v1", "day_stem", [_YOUNJIN[day_stem]] if day_stem in _YOUNJIN else None, ["intensity", "sharpness"])

    day_pillar = f"{day_stem or ''}{day_branch or ''}"
    if day_pillar in _KAIGOU:
        items.append(_shinsatsu_item(
            "魁罡", "kaigou_v1", "day_pillar", [("day", day_pillar)],
            hour_known=hour_known, tags=["strong_will", "extremes"],
        ))
    return items


def analyze_natal(
    stems: Iterable[NamedSymbol], branches: Iterable[NamedSymbol], *, hour_known: bool = True,
) -> tuple[list[dict], list[dict]]:
    """相互作用と神殺を一度に計算する便利関数。"""
    stem_entries = list(stems)
    branch_entries = list(branches)
    return (
        detect_interactions(stem_entries, branch_entries, hour_known=hour_known),
        detect_shinsatsu(stem_entries, branch_entries, hour_known=hour_known),
    )


def detect_cross_interactions(
    natal_stems: Iterable[NamedSymbol],
    natal_branches: Iterable[NamedSymbol],
    transit_stems: Iterable[NamedSymbol],
    transit_branches: Iterable[NamedSymbol],
    *,
    scope: str,
    hour_known: bool = True,
) -> list[dict]:
    """命式側と運側をまたぐ相互作用だけを検出する。"""
    natal_stem_entries = _normalize(natal_stems)
    natal_branch_entries = _normalize(natal_branches)
    transit_stem_entries = _normalize(transit_stems)
    transit_branch_entries = _normalize(transit_branches)
    items: list[dict] = []

    def append_scoped(detected: Iterable[dict]) -> None:
        for raw_item in detected:
            item = dict(raw_item)
            item["scope"] = scope
            items.append(item)

    for natal_entry in natal_stem_entries:
        for transit_entry in transit_stem_entries:
            append_scoped(detect_interactions(
                [natal_entry, transit_entry],
                [],
                hour_known=hour_known,
            ))

    for natal_entry in natal_branch_entries:
        for transit_entry in transit_branch_entries:
            append_scoped(detect_interactions(
                [],
                [natal_entry, transit_entry],
                hour_known=hour_known,
            ))

    input_order: dict[str, int] = {}
    for pillar, _ in (
        *natal_stem_entries,
        *natal_branch_entries,
        *transit_stem_entries,
        *transit_branch_entries,
    ):
        input_order.setdefault(pillar, len(input_order))

    completed_trines: list[frozenset[str]] = []
    natal_symbols = {symbol for _, symbol in natal_branch_entries}
    for group, element in _TRINES:
        group_set = frozenset(group)
        if group_set.issubset(natal_symbols):
            continue
        for transit_entry in transit_branch_entries:
            transit_name, transit_symbol = transit_entry
            if group_set - natal_symbols != {transit_symbol}:
                continue
            natal_chosen = [
                next(entry for entry in natal_branch_entries if entry[1] == symbol)
                for symbol in group
                if symbol != transit_symbol
            ]
            item = _interaction(
                rule_id="branch_trine_v1",
                type_="三合",
                category="branch",
                entries=[*natal_chosen, transit_entry],
                input_order=input_order,
                hour_known=hour_known,
                tags=["alliance", "amplification"],
                detail={
                    "element": element,
                    "transformation": "not_evaluated",
                    "completed_by": transit_name,
                },
            )
            item["scope"] = scope
            items.append(item)
            completed_trines.append(group_set)

    if completed_trines:
        items = [
            item
            for item in items
            if not (
                item["rule_id"] == "branch_half_trine_v1"
                and any(frozenset(item["members"]).issubset(group) for group in completed_trines)
            )
        ]

    for group, element in _DIRECTIONALS:
        group_set = frozenset(group)
        if group_set.issubset(natal_symbols):
            continue
        for transit_entry in transit_branch_entries:
            transit_name, transit_symbol = transit_entry
            if group_set - natal_symbols != {transit_symbol}:
                continue
            natal_chosen = [
                next(entry for entry in natal_branch_entries if entry[1] == symbol)
                for symbol in group
                if symbol != transit_symbol
            ]
            item = _interaction(
                rule_id="branch_directional_v1",
                type_="方合",
                category="branch",
                entries=[*natal_chosen, transit_entry],
                input_order=input_order,
                hour_known=hour_known,
                tags=["alliance", "seasonal_force"],
                detail={
                    "element": element,
                    "transformation": "not_evaluated",
                    "completed_by": transit_name,
                },
            )
            item["scope"] = scope
            items.append(item)
    return items


def detect_special_kanshi_relations(
    natal_pillars: Iterable[NamedSymbol],
    transit_kanshi: str,
    transit_name: str,
    *,
    scope: str,
    hour_known: bool = True,
) -> list[dict]:
    """運と命式の伏吟・反吟（天剋地冲）を検出する。"""
    natal_entries = _normalize(natal_pillars)
    input_order = {pillar: index for index, (pillar, _) in enumerate(natal_entries)}
    input_order.setdefault(transit_name, len(input_order))
    items: list[dict] = []

    for natal_name, natal_kanshi in natal_entries:
        if len(natal_kanshi) < 2 or len(transit_kanshi) < 2:
            continue
        rule_id = ""
        type_ = ""
        tags: list[str] = []
        if natal_kanshi == transit_kanshi:
            rule_id = "fukugin_v1"
            type_ = "伏吟"
            tags = ["repetition", "stagnation_or_emphasis"]
        elif (
            frozenset((natal_kanshi[0], transit_kanshi[0])) in _STEM_CLASHES
            and frozenset((natal_kanshi[1], transit_kanshi[1])) in _BRANCH_CLASHES
        ):
            rule_id = "hangin_v1"
            type_ = "反吟"
            tags = ["reversal", "upheaval"]
        if not rule_id:
            continue
        entries = _sort_entries(
            [(natal_name, natal_kanshi), (transit_name, transit_kanshi)],
            input_order,
        )
        pillars = [pillar for pillar, _ in entries]
        items.append({
            "rule_id": rule_id,
            "type": type_,
            "category": "kanshi",
            "scope": scope,
            "pillars": pillars,
            "members": [kanshi for _, kanshi in entries],
            "status": "detected",
            "provisional": not hour_known and "hour" in pillars,
            "tags": tags,
            "detail": {},
        })
    return items
