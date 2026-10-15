"""計算済みの流年を、鑑定者が検討できる根拠・質問へ整理する。"""
from __future__ import annotations

from typing import Any

PILLAR_LABELS = {"year": "年柱", "month": "月柱", "day": "日柱", "hour": "時柱", "daiun": "大運", "ryunen": "流年"}
GOD_LENSES = {
    "比肩": ("自立・自分の方針", "自分で決める範囲を明確にする", "独力で抱え込み、協力の余地を狭めていないか", "自分で決めたいことと、人に任せられることは何ですか"),
    "劫財": ("仲間・競争・資源の分担", "協働時の役割と分配を先に決める", "比較や付き合いで時間・支出が増えていないか", "共同で進めることの負担と取り分は合意できていますか"),
    "食神": ("表現・育成・日々の充実", "得意なことを続けられる形に整える", "心地よさを優先して締切や条件を曖昧にしていないか", "楽しみながら続けられる活動を、どう形にしたいですか"),
    "傷官": ("改善・専門性・言葉の精度", "違和感を具体的な改善案に変える", "正しさの主張が相手への批判になっていないか", "改善したい点を、相手が受け取れる言葉で説明できますか"),
    "偏財": ("交流・機会・資源の循環", "広がった機会を選別し、配分を決める", "頼まれごとや見込みだけで約束を増やしていないか", "増えた機会のうち、何を選び何を見送りますか"),
    "正財": ("継続・生活基盤・管理", "収支・時間・契約条件を見える形にする", "安定を守るために負担を引き受けすぎていないか", "今の生活や働き方を続けるために、見直したい条件は何ですか"),
    "偏官": ("決断・挑戦・外からの要求", "引き受ける課題と撤退条件を決める", "急ぎの要求に応えて消耗を見落としていないか", "その課題は今引き受ける必要がありますか。上限は決めていますか"),
    "正官": ("役割・責任・社会的な基準", "期待される役割と自分の裁量をすり合わせる", "評価や義務を優先して本音を抑えすぎていないか", "求められる役割と、自分が望む働き方は一致していますか"),
    "偏印": ("探究・発想の転換・独自の学び", "新しい視点を試し、使える形に整理する", "考える対象が広がり、実行や生活のリズムが崩れていないか", "学び直したいことを、どの場面で実際に使えそうですか"),
    "印綬": ("学習・支援・基盤の整備", "助言や学びを受け取り、判断の土台を整える", "準備や確認が増えて判断を先延ばしにしていないか", "今必要な支援は何ですか。準備を終える目安はありますか"),
}


def relation_text(item: dict[str, Any]) -> str:
    pillars = "・".join(PILLAR_LABELS.get(p, p) for p in item.get("pillars", []))
    members = "・".join(item.get("members", []))
    provisional = " ※出生時刻不明のため暫定" if item.get("provisional") else ""
    return f"{pillars}：{members} {item.get('type', '')}{provisional}"


def build_structure_themes(shichu: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = shichu.get("normalized_data") or {}
    structure = shichu.get("structure_report") or {}
    command = normalized.get("hidden_stem_command") or {}
    power = (structure.get("element_power") or {}).get("percent") or {}
    strength = structure.get("strength_v2") or {}
    patterns = (structure.get("pattern_candidates") or {}).get("candidates") or []
    yojin = structure.get("yojin_candidates") or {}
    evidence = []
    reasons = []
    if command.get("commanding_stem"):
        evidence.append(f"月令司令：{command['commanding_stem']}（{command.get('phase', '')}）、節入り後 {command.get('elapsed_days_since_jieqi', '')}日")
    if power:
        evidence.append("位置・蔵干を加味した五行力量：" + " / ".join(f"{e} {v}%" for e, v in power.items()))
    if strength:
        evidence.append(f"力量に基づく身強弱：{strength.get('band', '')}（指標 {strength.get('score', '')}）")
        if strength.get("special_pattern_suspicion"):
            reasons.append("特殊格の疑いあり。通常の身強弱による単純な補い方は確定しない。")
    for pattern in patterns:
        evidence.append(f"格局候補：{pattern['name']}（確度 {pattern.get('confidence', '')}）")
        reasons.extend(pattern.get("rationale") or [])
        reasons.extend("反証：" + text for text in pattern.get("counter_evidence") or [])
    for key, label in (("fu_yi", "扶抑"), ("climate", "調候")):
        candidate_set = yojin.get(key) or {}
        if candidate_set.get("status") in {"hold", "withheld"}:
            evidence.append(f"{label}の推奨五行：保留（{candidate_set.get('hold_reason') or candidate_set.get('status')}）")
        for candidate in candidate_set.get("candidates") or []:
            evidence.append(f"{label}の候補：{candidate['element']}（確度 {candidate.get('confidence', '')}）")
            reasons.extend(candidate.get("rationale") or [])
            reasons.extend("反証：" + text for text in candidate.get("counter_evidence") or [])
    if not evidence:
        return []
    reasons.append("五行の単純な個数と、季節・位置を加味した力量は別指標。扶抑と調候は役割を分け、格局や用神の確定判定にはしない。")
    if (normalized.get("timing") or {}).get("hour_known") is False:
        reasons.append("出生時刻不明のため時柱は仮定値。時柱を含む力量・格局候補・推奨五行も参考計算として扱う。")
    return [{
        "id": "shichu_structure_depth", "label": "月令・力量・格局・推奨五行の検討",
        "summary": "命式の強弱を、季節・蔵干・配置の根拠から検討する",
        "client_text": "得意な力の使い方と、負担が増えやすい条件を、今の暮らしに照らして整理します。",
        "reader_explanation": "表面的な五行不足だけで補う要素を決めず、月令・力量・格局候補・反証から読みを組み立てる。",
        "reasons": reasons, "evidence": evidence,
        "questions": ["普段は得意でも、忙しい時に負担へ変わることは何ですか"],
        "misread_risk": False,
    }]


def build_annual_themes(shichu: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = shichu.get("normalized_data") or {}
    timing = normalized.get("timing") or shichu.get("timing") or {}
    structure = shichu.get("structure_report") or {}
    yojin = (structure.get("yojin_candidates") or {}).get("fu_yi") or {}
    themes = []
    for item in (timing.get("ryunen") or {}).get("items", []):
        # 旧ログには年がなく、現在干支の略記だけの場合がある。
        if not isinstance(item, dict) or not item.get("year") or not item.get("kanshi"):
            continue
        god = item.get("stem_ten_god", "")
        focus, action, caution, question = GOD_LENSES.get(god, (
            "計算データの確認", "命式と現状を照合する", "通変星データが不足していないか", "今、最も気になっている変化は何ですか",
        ))
        evidence = [f"流年 {item['year']}年 {item['kanshi']}：{god}／十二運 {item.get('branch_twelve_fortune', '')}"]
        if item.get("starts_at"):
            evidence.append(f"適用期間：{item['starts_at']} 〜 {item['ends_at']}（終端を含まない）")
        daiun = item.get("current_daiun") or {}
        if daiun:
            evidence.append(f"{item.get('reference_date', timing.get('target_date', ''))}時点の大運：{daiun['kanshi']} {daiun.get('stem_ten_god', '')}（簡易起運）")
        else:
            evidence.append("参照日時点の大運は未確定／起運前・計算範囲外。大運との重ね読みは保留。")
        hidden = item.get("hidden_stems") or []
        if hidden:
            evidence.append("流年支の蔵干：" + "・".join(f"{h['stem']}（{h['ten_god']}）" for h in hidden))
        relations = [rel for key in ("interactions_natal", "interactions_daiun", "interactions_combined", "special_natal", "special_daiun", "three_punishments")
                     for rel in item.get(key, [])]
        evidence.extend(dict.fromkeys(relation_text(rel) for rel in relations))
        reasons = [
            f"年干の{god or '通変星未取得'}から『{focus}』を検討する。年単独で吉凶を決めず、命式の強弱・月令・格局候補と照合する。",
            f"行動案：{action}。現状確認：{caution}。",
            "十二運は勢いの補助指標。合・三合・方合は合化成立を意味せず、冲・刑・害・破も出来事の確定予測にしない。",
        ]
        if item.get("daiun_changes_in_year"):
            reasons.append("この流年内には起運・大運切替の目安がある。年全体を一つの大運で断定せず、前後を分けて確認する：" +
                           " → ".join(d["kanshi"] for d in item.get("daiun_overlaps", [])))
            for context in item.get("daiun_overlaps", []):
                evidence.append(f"切替前後の大運候補：{context['kanshi']}（{context['age_from']}〜{context['age_to']}歳頃）")
                evidence.extend(f"{context['kanshi']}の期間：{relation_text(rel)}"
                                for key in ("interactions_daiun", "interactions_combined", "special_daiun", "three_punishments")
                                for rel in context.get(key, []))
        if item.get("kubo"):
            evidence.append("流年支が日柱基準の空亡に該当")
            reasons.append("空亡は補助情報として扱い、凶年・損失・失敗を断定しない。")
        if yojin.get("status") in {"hold", "withheld"}:
            reasons.append("扶抑の推奨五行は保留。流年の五行を用神・忌神と断定しない。")
        elif yojin.get("candidates"):
            elements = {item.get("stem_element"), item.get("branch_element")}
            matched = [c["element"] for c in yojin["candidates"] if c.get("element") in elements]
            if matched:
                reasons.append("流年の五行と扶抑候補の重なり：" + "・".join(matched) + "。候補との一致だけで吉年と判定しない。")
        if timing.get("hour_known") is False:
            reasons.append("出生時刻不明。時柱を含む作用は暫定で、起運・大運切替にも時刻仮定の影響がある。")
        themes.append({
            "id": f"shichu_annual_{item['year']}",
            "label": f"{item['year']}年の年運（立春基準）" + ("・基準日の流年" if item.get("is_current") else ""),
            "summary": f"{item['kanshi']}・{god}：{focus}",
            "client_text": f"この年は『{focus}』を見直す視点が使えます。{action}ことを、今の状況に合わせて検討しましょう。",
            "reader_explanation": "生来の傾向→大運の背景→流年の刺激→現状との照合→行動案の順で説明する。年運は性格の断定に使わない。",
            "reasons": reasons,
            "evidence": evidence,
            "questions": [question, "仕事・お金・恋愛や家族関係のうち、実際に変化を感じているのはどの領域ですか"],
            "misread_risk": bool(any(r.get("provisional") for r in relations)),
            "misread_note": "出生時刻不明の時柱を含む作用は暫定です。",
        })
    return themes
