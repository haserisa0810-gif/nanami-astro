from __future__ import annotations

from typing import Any

CHILD_SYSTEM_PROMPT = """
あなたは子どもの特性を読み解く占星術家です。

・対象は0〜12歳
・すべて「傾向」で表現
・不安を煽らない
・保護者にも伝わる、やさしく具体的な言葉を使う

出力構成：
1. 基本気質
2. 才能の芽
3. 安心できる環境
4. 関わり方のコツ
5. 表現の出口
""".strip()

TEEN_SYSTEM_PROMPT = """
あなたは思春期の自己理解をサポートする占星術家です。

・断定しない
・本人が理解できる言葉
・安心感を優先
・説教ではなく、自己理解の補助として書く

出力：
1. 基本スタイル
2. 強みと使い方
3. ハマりやすい状態
4. 対人ヒント
5. モヤモヤの正体
6. 今の過ごし方
""".strip()

ADULT_SYSTEM_PROMPT = """
あなたは統合占術の専門家です。

・西洋を主軸
・四柱推命で現実化
・インドは補助

構造的に分析し、実用的なアドバイスを行う
""".strip()


SINGLE_SYSTEM_NAMES = {"western": "西洋占星術", "shichusuimei": "四柱推命", "vedic": "インド占星術"}


def get_age_system_prompt(age_mode: str, *, astrology_system: str | None = None) -> str:
    age_mode = str(age_mode or "adult").strip().lower()
    if age_mode == "child":
        prompt = CHILD_SYSTEM_PROMPT
    elif age_mode == "teen":
        prompt = TEEN_SYSTEM_PROMPT
    else:
        name = SINGLE_SYSTEM_NAMES.get(str(astrology_system or "").strip().lower())
        if name:
            return f"あなたは{name}の専門家です。\n\n入力された{name}のデータだけを構造的に分析し、実用的なアドバイスを行う。"
        return ADULT_SYSTEM_PROMPT
    if astrology_system == "shichusuimei":
        prompt = prompt.replace("占星術家", "四柱推命の鑑定者")
    return prompt


def _distribution_value(distribution: dict[str, Any], key: str, default: int) -> int:
    value = distribution.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"Invalid distribution value: {key}")
    try:
        number = int(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"Invalid distribution value: {key}") from exc
    if number < 0 or (isinstance(value, float) and value != number):
        raise ValueError(f"Invalid distribution value: {key}")
    return number


def build_role_prompt(
    distribution: dict[str, Any] | None,
    *,
    age_mode: str = "adult",
    structured_mode: str = "general",
    vedic_level: str = "off",
    astrology_system: str | None = None,
) -> str:
    system = str(astrology_system or "").strip().lower()
    name = SINGLE_SYSTEM_NAMES.get(system)
    if name:
        return f"【使用する占術】\n{name}のみを主軸として使用する。\n他の占術は使用せず、入力された{name}のデータだけを根拠にする。"

    distribution = distribution if distribution is not None else {}
    western = _distribution_value(distribution, "western", 45)
    shichu = _distribution_value(distribution, "shichu", 45)
    vedic = _distribution_value(distribution, "vedic", 10)
    if western + shichu + vedic == 0:
        raise ValueError("At least one astrology distribution value must be positive")

    if western >= shichu + 8:
        western_role = "・主軸として使用\n・性格・感情・対人関係を中心に描写"
        shichu_role = "・補助として使用\n・現実的な行動・仕事・人生構造を説明"
    elif shichu >= western + 8:
        western_role = "・補助として使用\n・感情や対人のニュアンスを補足"
        shichu_role = "・主軸寄りの補助として使用\n・現実的な行動・仕事・人生構造を説明"
    else:
        western_role = "・主軸として使用\n・性格・感情・対人関係の読みの中心に置く"
        shichu_role = "・現実面の補助として使用\n・行動・仕事・生活設計を具体化する"

    if western == 0:
        western_role = "・使用しない"
        if shichu > 0:
            shichu_role = "・主軸として使用\n・入力された四柱推命データを根拠に読む"
    if shichu == 0:
        shichu_role = "・使用しない"

    if age_mode in {"child", "teen"}:
        vedic_role = "・必要時のみ補助的に使用\n・重い言葉は使わず、背景の流れを短く補足\n・最大2文以内"
    else:
        vedic_role = "・補助的に使用\n・理由や背景の説明に限定\n・最大2文以内\n・主軸にしない\n・『宿命』『カルマ』は使わず『流れ』『パターン』で表現"

    structured_extra = ""
    if str(structured_mode or "general") == "structured":
        structured_extra = """
【structured判定】
・一般論の並べ直しを避ける
・矛盾して見える性質を、そのまま二層構造で書く
・単体要素ではなく、組み合わせで読む
""".strip()

    vedic_extra = ""
    if str(vedic_level or "off") == "off":
        vedic_extra = "・インド占星術は今回ほぼ前面に出さず、必要なときだけ背景説明として添える"
    elif str(vedic_level or "off") == "light":
        vedic_extra = "・インド占星術は背景説明の補助に限定し、全体を支配しない"
    else:
        vedic_extra = "・インド占星術は背景の流れを短く示してよいが、主役にしない"

    if vedic == 0:
        vedic_role = "・使用しない"
        vedic_extra = ""

    base = f"""
この鑑定では以下の役割で占術を使用してください：

【西洋占星術】
{western_role}

【四柱推命】
{shichu_role}

【インド占星術】
{vedic_role}
{vedic_extra}

【内部メモ】
・配合比の数値（西洋:{western} / 四柱:{shichu} / インド:{vedic}）は内部制御用です
・本文では割合を書かず、役割として自然に統合してください
""".strip()
    return base + ("\n\n" + structured_extra if structured_extra else "")
