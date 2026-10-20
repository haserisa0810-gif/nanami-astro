from __future__ import annotations

STYLE_PROMPTS = {
    'general': "",
    'structured': """【特化指示：構造を組み合わせて読む】
主要構造を最大2つ選び、複数の配置・作用の関係から、動機、判断基準、力が出る条件、条件が変わったときの現れ方までつなげる。
単体要素の性格辞典で終えず、支える構造や反証も読む。
矛盾がある場合は両方の働く条件を示し、一致する場合はその安定を具体化する。特別に複雑な人、外面と内面が違う人という前提を置かない。
根拠のある厳しい傾向を消さず、根拠のない問題や経験を追加しない。""",
    'love': """【特化指示：恋愛・関係性】
関わる動機、惹かれる条件、距離と相互性、安定して続く条件を、主要構造から具体化する。
葛藤・執着・関係が崩れる流れは、支持する根拠がある場合にだけ、その条件と行動の出方を説明する。
傷ついていても離れられない、自己犠牲になる、関係を壊すという経験を前提にしない。一致や安定も読む。
本人の実体験を捏造せず、確かめる質問には問題の存在を埋め込まない。""",
    'work': """【特化指示：仕事】
能力が発揮される役割、判断の基準、裁量、評価基準との適合、資源の使い方を主要構造から読む。
負荷や失敗の条件は根拠がある場合に具体化し、安定して働ける条件と区別する。
職場での生きづらさやストレスを必須項目にせず、一般的な適職一覧で埋めない。収入や成功を保証しない。""",
    'deep': """【特化指示：日常に届く深読み】
構造と根拠から、何を大切にしやすいか、何を基準に決めるか、場面によって同じ力がどう変わるかを具体化する。
具体的な仮の場面は例として示し、支える要素・反証・例外も添える。
体感的な言葉は読解した根拠に沿って使い、共感されそうな悩みや過去の出来事を作らない。
問題の深刻さや言い切りの強さで「当たっている感じ」を演出しない。根拠の強い葛藤は明確に書く。""",
}


GENERAL_READING_STYLES = {"general", "structured", ""}


def get_style_prompt(reading_style: str, astrology_system: str) -> str:
    return STYLE_PROMPTS.get(reading_style, "")


def resolve_reading_style(reading_style: str | None, suggested_style: str | None) -> str:
    """鑑定スタイルを確定する。

    画面上は「総合鑑定」1つだけを選ばせ、構造読解（structured）の指示を付けるかは
    出生図の判定（suggested_reading_style）で決める。手動モードでも同じ。
    旧画面・過去データの "structured" も総合鑑定として扱い直す。
    恋愛・仕事などの特化スタイルはそのまま返す。
    """
    style = (reading_style or "general").strip().lower()
    if style not in GENERAL_READING_STYLES:
        return style
    return "structured" if (suggested_style or "").strip().lower() == "structured" else "general"
