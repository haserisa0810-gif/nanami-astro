from __future__ import annotations

from copy import deepcopy
from typing import Any


# Source: dev/nanami-type/services/type_diagnosis.py TYPE_CATALOG.
# Keep IDs, labels, summaries, and subtype labels aligned with the public /type diagnosis.
TYPE_CATALOG: dict[str, dict[str, Any]] = {
    "backstage_leader": {
        "label": "裏方リーダー型",
        "summary": "表に出なくても、全体を見渡して最適に動かせる調整力の持ち主。周囲を活かしながら成果を出す、静かなリーダータイプ。",
        "subtypes": {"analyst": "分析特化", "empathic_coordinator": "共感巻き込み", "perfect_architect": "完璧設計"},
        "keywords": ["調整力", "全体把握", "静かなリーダーシップ"],
        "western_view": "西洋占星術では、風・地の論理性や対人ハウスの働きが出やすいタイプとして扱います。",
        "vedic_view": "インド占星術では、関係性・役割意識・支援領域の出方を補助線として見ます。",
        "shichu_view": "四柱推命では、印星・官星・財星などのバランスから、整える力や責任感の出方を見ます。",
        "strengths": ["場を見渡して調整できる", "人を活かしながら成果につなげられる"],
        "tendencies": ["自分が前に出るより、全体の流れを整えやすい"],
        "cautions": ["抱え込みすぎる前に、役割を分けると動きやすい"],
    },
    "self_doubt_loop": {
        "label": "自己探求ループ型",
        "summary": "自分を深く見つめる力があり、成長意欲がとても高いタイプ。納得いくまで磨き続けることで、大きく伸びる可能性を持つ。",
        "subtypes": {"comparison_drop": "比較落ち", "perfection_denial": "完璧否定", "past_holding": "過去引きずり"},
        "keywords": ["内省", "成長意欲", "納得感"],
        "western_view": "西洋占星術では、論理性と内向きの集中力、自己検証の傾向を見ます。",
        "vedic_view": "インド占星術では、内面圧や精神性、学び直しのテーマを補助線として見ます。",
        "shichu_view": "四柱推命では、印星や身強弱の出方から、考え込みや磨き込みの癖を見ます。",
        "strengths": ["深く考えて改善できる", "納得するまで質を高められる"],
        "tendencies": ["比較や過去の記憶から、自分を見直しやすい"],
        "cautions": ["答えを出す前に小さく試すと、思考が巡りすぎにくい"],
    },
    "breakthrough_burnout": {
        "label": "突破集中型（燃焼タイプ）",
        "summary": "一気に物事を前進させる爆発的な推進力の持ち主。ここぞという場面で流れを変えられるエネルギーが強み。",
        "subtypes": {"sprint_fullpower": "短距離全力", "scattered_energy": "分散エネルギー", "external_pressure": "外圧依存"},
        "keywords": ["推進力", "集中突破", "瞬発力"],
        "western_view": "西洋占星術では、火の勢いや活動ハウスの強さを中心に見ます。",
        "vedic_view": "インド占星術では、実行テーマや配置の強弱から、動き出す力の出方を見ます。",
        "shichu_view": "四柱推命では、火・比劫・食傷などから、前へ出る力や燃焼ペースを見ます。",
        "strengths": ["停滞した流れを動かせる", "短期集中で成果を出しやすい"],
        "tendencies": ["勢いが出るほど、休むタイミングを後回しにしやすい"],
        "cautions": ["全力を続けるより、区切りを作ると力が残りやすい"],
    },
    "empathy_absorb": {
        "label": "共感共鳴型",
        "summary": "人の感情や空気を繊細に感じ取れる共感力の高いタイプ。人との繋がりや信頼関係を深く築くことができる。",
        "subtypes": {"over_empathy": "過剰共感", "air_priority": "空気優先", "rescuer": "救済者"},
        "keywords": ["共感力", "感受性", "信頼関係"],
        "western_view": "西洋占星術では、水の感受性や対人ハウスの反応を中心に見ます。",
        "vedic_view": "インド占星術では、関係性・心の負荷・支援テーマの出方を見ます。",
        "shichu_view": "四柱推命では、水や傷官、官財の出方から、感情の受け取り方を見ます。",
        "strengths": ["相手の気持ちに寄り添える", "深い信頼関係を築きやすい"],
        "tendencies": ["周囲の空気を受け取り、自分の気持ちと混ざりやすい"],
        "cautions": ["境界線を言葉にすると、優しさを消耗しにくい"],
    },
    "perfectionist": {
        "label": "高品質追求型",
        "summary": "細部までこだわり、質の高いものを生み出せる職人気質。完成度を引き上げる力に優れている。",
        "subtypes": {"ideal_standard": "理想基準", "mistake_avoidance": "ミス回避", "evaluation_pressure": "他人評価"},
        "keywords": ["品質", "改善", "職人気質"],
        "western_view": "西洋占星術では、地の現実感や論理性、細部を見る力を中心に見ます。",
        "vedic_view": "インド占星術では、安定度や役割意識から、質を整える力の出方を見ます。",
        "shichu_view": "四柱推命では、金・土・印星などから、精度や基準の高さを見ます。",
        "strengths": ["細部を整えて完成度を上げられる", "信頼される品質を作りやすい"],
        "tendencies": ["理想基準が高く、仕上げに時間をかけやすい"],
        "cautions": ["完璧にする前に一度出すと、次の改善点が見えやすい"],
    },
    "stability_dependent": {
        "label": "安定構築型",
        "summary": "安心できる基盤を作るのが得意で、継続力があるタイプ。堅実に積み上げていくことで、大きな信頼を得られる。",
        "subtypes": {"status_quo": "現状維持", "anxiety_avoid": "不安回避", "environment_dependent": "環境依存"},
        "keywords": ["継続力", "基盤作り", "堅実さ"],
        "western_view": "西洋占星術では、地の安定感や守る力を中心に見ます。",
        "vedic_view": "インド占星術では、配置の安定度や継続テーマを補助線として見ます。",
        "shichu_view": "四柱推命では、土・金・身強弱から、積み上げる力と慎重さを見ます。",
        "strengths": ["継続して信頼を作れる", "安心できる土台を整えられる"],
        "tendencies": ["変化よりも、確実に続けられる形を選びやすい"],
        "cautions": ["小さな変更から始めると、不安を抱えず動きやすい"],
    },
    "passive_chance": {
        "label": "タイミングキャッチ型",
        "summary": "流れを読む力があり、最適な瞬間を見極めるセンスの持ち主。無駄打ちせず、ここぞで動ける慎重さが強み。",
        "subtypes": {"waiting_watch": "様子見", "others_first": "受動依存", "fail_stop": "失敗回避停止"},
        "keywords": ["タイミング", "慎重さ", "流れを読む力"],
        "western_view": "西洋占星術では、受け取る力や対人・環境の流れを読む配置を見ます。",
        "vedic_view": "インド占星術では、時期テーマや支援領域から、動くタイミングを見ます。",
        "shichu_view": "四柱推命では、印星・土・金などから、様子を見る力と選択の慎重さを見ます。",
        "strengths": ["必要な時に動ける", "無駄な消耗を避けやすい"],
        "tendencies": ["確信が持てるまで様子を見やすい"],
        "cautions": ["小さな合図で動く練習をすると、機会を逃しにくい"],
    },
    "approval_dependent": {
        "label": "共鳴評価型",
        "summary": "相手の反応やニーズを的確に捉えられる対人感覚の鋭いタイプ。人に求められるものを形にする力に長けている。",
        "subtypes": {"reaction_sensitive": "評価敏感", "expectation_overreply": "期待過剰応答", "comparison_compete": "比較競争"},
        "keywords": ["対人感覚", "ニーズ把握", "共鳴力"],
        "western_view": "西洋占星術では、対人ハウスや風・火の反応力を中心に見ます。",
        "vedic_view": "インド占星術では、関係性・社会的評価・役割テーマの出方を見ます。",
        "shichu_view": "四柱推命では、官星・財星・比劫から、評価や期待への反応を見ます。",
        "strengths": ["相手が求めるものを形にできる", "場の反応を読んで調整できる"],
        "tendencies": ["評価や反応によって、動き方が変わりやすい"],
        "cautions": ["先に自分の基準を置くと、期待に振り回されにくい"],
    },
    "solo_fighter": {
        "label": "自走突破型",
        "summary": "一人でも結果を出せる実行力と集中力を持つタイプ。自分の力で道を切り開く強さがある。",
        "subtypes": {"self_complete": "完全自己完結", "trust_issue": "信頼不全", "isolated_endurance": "孤立耐久"},
        "keywords": ["自走力", "集中力", "独立性"],
        "western_view": "西洋占星術では、自己主導の配置や活動ハウスの強さを中心に見ます。",
        "vedic_view": "インド占星術では、自己実行テーマやキャリア方向の出方を見ます。",
        "shichu_view": "四柱推命では、比劫・火・金などから、自力で進める力を見ます。",
        "strengths": ["一人でも前へ進める", "集中して結果につなげやすい"],
        "tendencies": ["頼る前に自分で抱えて進めやすい"],
        "cautions": ["一部だけ任せると、集中力を大事な所に残しやすい"],
    },
    "ideal_first": {
        "label": "ビジョン先導型",
        "summary": "未来を描く力があり、人や流れを導ける発想力の持ち主。理想を現実に近づける原動力になる。",
        "subtypes": {"vision_overload": "ビジョン過多", "ideal_gap": "理想現実ギャップ", "postpone_action": "行動後回し"},
        "keywords": ["ビジョン", "発想力", "未来志向"],
        "western_view": "西洋占星術では、風・火の可能性を見る力や未来志向を中心に見ます。",
        "vedic_view": "インド占星術では、学び・精神性・方向転換テーマの出方を見ます。",
        "shichu_view": "四柱推命では、木・水・食傷などから、構想力と展開力を見ます。",
        "strengths": ["未来像を描いて人や流れを導ける", "理想を言葉にして動きを作れる"],
        "tendencies": ["構想が広がるほど、着手が後回しになりやすい"],
        "cautions": ["最初の一手を小さく決めると、理想が現実に近づきやすい"],
    },
}


def get_type_catalog() -> dict[str, dict[str, Any]]:
    return deepcopy(TYPE_CATALOG)


def get_type_definitions_for_prompt() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for type_id, item in TYPE_CATALOG.items():
        rows.append(
            {
                "type_id": type_id,
                "type_name": item["label"],
                "summary_for_reference_only": item["summary"],
                "keywords": item["keywords"],
                "western_view": item["western_view"],
                "vedic_view": item["vedic_view"],
                "shichu_view": item["shichu_view"],
                "strengths": item["strengths"],
                "tendencies": item["tendencies"],
                "cautions": item["cautions"],
                "subtypes": item["subtypes"],
            }
        )
    return rows


def get_type_subtype_combinations_for_prompt() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for type_id, item in TYPE_CATALOG.items():
        for subtype_id, subtype_label in item["subtypes"].items():
            rows.append(
                {
                    "type_id": type_id,
                    "type_name": item["label"],
                    "subtype_id": subtype_id,
                    "subtype_name": subtype_label,
                    "display_name": f"{item['label']} × {subtype_label}",
                    "parent_summary_for_reference_only": item["summary"],
                    "parent_keywords": item["keywords"],
                    "parent_tendencies": item["tendencies"],
                    "parent_cautions": item["cautions"],
                }
            )
    return rows


def get_type_subtype_groups_for_prompt() -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for type_id, item in TYPE_CATALOG.items():
        combinations = []
        for subtype_id, subtype_label in item["subtypes"].items():
            combinations.append(
                {
                    "type_id": type_id,
                    "type_name": item["label"],
                    "subtype_id": subtype_id,
                    "subtype_name": subtype_label,
                    "display_name": f"{item['label']} × {subtype_label}",
                    "parent_keywords": item["keywords"],
                    "parent_tendencies": item["tendencies"],
                    "parent_cautions": item["cautions"],
                }
            )
        groups.append(
            {
                "type_id": type_id,
                "type_name": item["label"],
                "parent_summary_for_reference_only": item["summary"],
                "parent_keywords": item["keywords"],
                "parent_tendencies": item["tendencies"],
                "parent_cautions": item["cautions"],
                "combinations": combinations,
            }
        )
    return groups
