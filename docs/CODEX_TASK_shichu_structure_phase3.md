# タスク: 四柱推命 月律分野蔵干・力量評価・格局候補・用神候補（フェーズ3）

あなたはこのリポジトリ（nanami-astro）の実装担当です。フェーズ1（相互作用エンジン）・フェーズ2（大運・流年）に続き、命式の構造分析を深化させます。

## 前提（必ず読むこと）

- `services/shichusuimei_calc.py` — `STEM_ELEMENT` / `BRANCH_ELEMENT` / `HIDDEN_STEMS` / `ten_god()` / `_element_generate()` / `_element_control()` / `_jieqi_boundary_utc_for_birth_month()` / `_calc_strength_score()` を再利用する
- `services/shichu_interactions.py` — 冲テーブル `_BRANCH_CLASHES` を再利用する
- 設計思想: 計算事実＋タグのみ、**断定せず候補として出す（confidence＋根拠＋反証）**、流派差は assumptions 明記、追加キーのみで後方互換維持
- **既存の `_calc_strength_score()`・`main_hidden_stems`・`structure_report` の既存キーは一切変更しない**。新しい判定は新キーとして併存させる

## 成果物

1. 新規モジュール `services/shichu_structure.py` — 月律分野蔵干・力量評価・格局候補・用神候補
2. `services/shichusuimei_calc.py` — 統合（追加キーのみ）、`engine_version` を `0.5.0` に
3. 新規テスト `tests/test_shichu_structure.py`
4. `services/shichu_formatter.py` — 司令・格局候補・推奨五行候補の1行表示を追加
5. w_shichu 系プロンプト6ファイルに読み方を追記

---

## 1. 月律分野蔵干（司令）

### 採用表（この表が正。改変・補完禁止。流派差があることを assumptions に明記）

節入りからの経過日数で司令する蔵干が変わる。累積日数方式（30日基準、経過日数が表の合計を超える場合は本気司令）:

| 月支 | 余気 | 中気 | 本気 |
|---|---|---|---|
| 子 | 壬 10日 | — | 癸 20日 |
| 丑 | 癸 9日 | 辛 3日 | 己 18日 |
| 寅 | 戊 7日 | 丙 7日 | 甲 16日 |
| 卯 | 甲 10日 | — | 乙 20日 |
| 辰 | 乙 9日 | 癸 3日 | 戊 18日 |
| 巳 | 戊 7日 | 庚 7日 | 丙 16日 |
| 午 | 丙 10日 | 己 9日 | 丁 11日 |
| 未 | 丁 9日 | 乙 3日 | 己 18日 |
| 申 | 戊 7日 | 壬 7日 | 庚 16日 |
| 酉 | 庚 10日 | — | 辛 20日 |
| 戌 | 辛 9日 | 丁 3日 | 戊 18日 |
| 亥 | 戊 7日 | 甲 7日 | 壬 16日 |

判定: 経過日数 < 余気日数 → 余気司令。< 余気＋中気 → 中気司令。それ以外 → 本気司令。（例: 卯月で経過9.5日 → 甲司令、10.5日 → 乙司令）

### 実装

```python
def month_command(month_branch: str, elapsed_days: float) -> dict:
    # 戻り値:
    {
        "month_branch": "寅",
        "elapsed_days_since_jieqi": 8.2,   # 呼び出し側から受けた値をそのまま格納
        "commanding_stem": "丙",           # 司令蔵干
        "phase": "中気",                   # 余気 | 中気 | 本気
        "composition": [                   # 表の全構成（順序は余気→中気→本気）
            {"stem": "戊", "phase": "余気", "days": 7},
            {"stem": "丙", "phase": "中気", "days": 7},
            {"stem": "甲", "phase": "本気", "days": 16},
        ],
        "rule_id": "month_command_v1",
    }
```

- 経過日数は統合側で `_jieqi_boundary_utc_for_birth_month()` の前境界（節入り時刻）と出生UTCの差から求める
- **既存の `HIDDEN_STEMS`（固定表）と `main_hidden_stems` は変更しない**。新キー `hidden_stem_command` として併存させる（GPT合意事項）

## 2. 五行力量評価（element_power）

命式全体の五行パワーを重み付きで算出する。**手計算で検証可能なよう、重みは以下に固定**:

- 天干（年・月・日・時の4つすべて）: 各 **1.0**
- 地支の蔵干（フェーズ1の固定表 `HIDDEN_STEMS` を使用）: 本気 **1.0**・中気 **0.5**・余気 **0.3**
  - 蔵干の順序: `HIDDEN_STEMS` のリストは [本気, 中気, 余気] の順（1件なら本気のみ、2件なら本気＋中気）
- 柱の位置係数（地支の蔵干に乗算）: 月支 **×2.0**・日支 **×1.2**・時支 **×1.0**・年支 **×0.8**
- 月律司令の蔵干（月支内）: さらに **＋0.5** 加算

出力:

```python
{
    "rule_id": "element_power_v1",
    "raw": {"木": 3.1, "火": 2.0, "土": 4.4, "金": 1.0, "水": 2.3},
    "percent": {"木": 24.2, ...},          # raw合計に対する百分率（小数1桁）
    "weights_note": "天干1.0/蔵干(本気1.0,中気0.5,余気0.3)/位置係数(月2.0,日1.2,時1.0,年0.8)/司令+0.5",
}
```

### 日主サポート比と身強弱スコアv2

- supporters = 日干と同五行のパワー ＋ 日干を生じる五行（印）のパワー。**ただし日干自身の天干分1.0は除外**
- drainers = 残り3五行（食傷・財・官殺に相当）のパワー合計
- `support_ratio = supporters / (supporters + drainers)`
- `strength_score_v2 = round(support_ratio * 100)`

```python
{
    "rule_id": "strength_v2",
    "score": 58,
    "supporters": {"elements": ["金", "土"], "power": 4.2},
    "drainers": {"elements": ["水", "木", "火"], "power": 3.0},
    "band": "中庸域",     # score >= 65: "強め域" / <= 35: "弱め域" / それ以外: "中庸域"
    "special_pattern_suspicion": bool,   # 下記
    "suspicion_reasons": [...],
}
```

- `special_pattern_suspicion` は次のいずれかで True: `score >= 85`、`score <= 15`、いずれかの五行が percent で **55%以上**。理由文字列（"従格・専旺の可能性" 等）を suspicion_reasons に入れる
- **既存の `_calc_strength_score()`（v1）はそのまま残し、両方出力する**

## 3. 格局候補（pattern_candidates）

普通格局のみ。外格（従格・化格等）は判定せず、suspicion フラグで保留するだけ。

### 判定ルール

日干→建禄支: 甲→寅 / 乙→卯 / 丙→巳 / 丁→午 / 戊→巳 / 己→午 / 庚→申 / 辛→酉 / 壬→亥 / 癸→子
月刃（陽干のみ、フェーズ1の `_YOUNJIN` と同表）: 甲→卯 / 丙→午 / 戊→午 / 庚→酉 / 壬→子

1. 月支が日干の建禄支 → **建禄格**（confidence: "high"）
2. 月支が日干の月刃支（陽干のみ） → **月刃格**（confidence: "high"）
3. 上記以外: 月支の蔵干（月律の composition 全体）それぞれについて、年干・月干・時干への**透干**を調べる:
   - 本気が透干 → その十神の格（confidence: "high"）
   - 中気・余気が透干 → その十神の格（confidence: "medium"）
   - どれも透干しない → 本気の十神の格（confidence: "medium"）
   - **比肩・劫財に当たる蔵干は格候補にしない**（建禄格・月刃格で扱うため）
4. 格名は十神名＋「格」: 食神格・傷官格・偏財格・正財格・偏官格・正官格・偏印格・印綬格

### 反証（counter_evidence）

- 月支が他の地支と冲している（`_BRANCH_CLASHES` で判定）→ `"月支に冲があり格の成立が揺らぐ"`
- `special_pattern_suspicion == True` → `"従格・専旺の可能性があり普通格局の前提が崩れる場合がある"`

### 出力

```python
{
    "rule_id": "pattern_v1",
    "candidates": [   # confidence 降順（high → medium）
        {
            "name": "正官格",
            "confidence": "high",
            "basis": {"month_branch": "卯", "hidden_stem": "乙", "phase": "本気", "transparent_in": ["month"]},
            "rationale": ["月支本気の乙(正官)が月干に透干"],
            "counter_evidence": [],
        },
    ],
    "policy": "普通格局のみ。確定ラベルではなく候補。外格は判定しない",
}
```

## 4. 用神候補（扶抑と調候を分離。GPT合意: 名称は「推奨五行候補」）

### 扶抑候補（fu_yi_candidates）

strength_score_v2 に基づく。日干の五行を基準に:

- **強め域（score >= 65）**: 候補 = 泄（日干が生じる五行＝食傷）・耗（日干が剋す五行＝財）・制（日干を剋す五行＝官殺）の3つ、各 confidence "medium"
- **弱め域（score <= 35）**: 候補 = 印（日干を生じる五行）・比劫（日干と同五行）の2つ、各 confidence "medium"
- **中庸域（36〜64）**: 候補を出さず `status: "hold"`, `hold_reason: "身強弱が境界域のため自動判定を保留"`
- **special_pattern_suspicion == True**: 候補を出さず `status: "withheld"`, `hold_reason: "従格・専旺の可能性があるため通常の扶抑を適用しない"`（境界域判定より優先）

```python
{
    "rule_id": "fu_yi_v1",
    "status": "proposed",   # proposed | hold | withheld
    "hold_reason": None,
    "candidates": [
        {"element": "火", "role": "食傷(泄)", "confidence": "medium",
         "rationale": ["身強寄り(score 71)のため日干の力を流す五行を候補とする"],
         "counter_evidence": ["調候候補と競合する場合は調候を優先検討"]},
    ],
    "policy": "扶抑上の推奨五行候補。用神の確定ではない",
}
```

### 調候候補（climate_candidates）— 簡易版

窮通宝鑑の完全表（日干×月支120通り）は今回**採用しない**（表の検証コストが高いため将来対応）。月支の季節軸による簡易版とし、rule_id で明示する:

| 月支 | 候補五行 | confidence | 理由 |
|---|---|---|---|
| 亥・子・丑 | 火 | medium | 寒月のため暖が必要 |
| 巳・午・未 | 水 | medium | 暑月のため潤が必要 |
| 寅 | 火 | low | 余寒が残る |
| 申 | 水 | low | 残暑が残る |
| 卯・辰・酉・戌 | （候補なし） | — | — |

```python
{
    "rule_id": "climate_simplified_v1",
    "candidates": [...],   # 上記表。候補なし月は空リスト
    "policy": "季節軸の簡易調候。窮通宝鑑の完全表は未実装",
}
```

### 一致・競合の表示

`fu_yi_candidates` と `climate_candidates` の両方が候補を持つ場合、要素が一致するか競合するかを示す:

```python
"agreement": {
    "matched_elements": ["火"],       # 両方に出る五行
    "conflicted": bool,               # 両者に共通要素が1つもない場合 True
}
```

## 5. `calc_shichusuimei_from_payload` への統合

- 出力に追加（既存キー変更禁止）:
  - `normalized_data["hidden_stem_command"]` — month_command の戻り値
  - `structure_report` に追加キー: `"element_power"`, `"strength_v2"`, `"pattern_candidates"`, `"yojin_candidates"`（= `{"fu_yi": {...}, "climate": {...}, "agreement": {...}}`）
- 経過日数の計算: `_jieqi_boundary_utc_for_birth_month()` の前境界と出生UTCの差を日数（float）で渡す
- `input.assumptions` に追記:
  - `"month_command_rules": "月律分野蔵干は累積日数方式(30日基準)。流派差あり"`
  - `"yojin_rules": "扶抑=strength_v2ベースの推奨五行候補(境界域・従格疑いは保留)。調候=季節軸の簡易版。いずれも用神の確定ではない"`
- `engine_version` を `"0.5.0"` に

## 6. テスト `tests/test_shichu_structure.py`

月律・力量・格局・用神はエフェメリス不要（干支・経過日数を直接指定）で:

1. **月律の境界**: 卯月 elapsed 9.5日 → 甲司令(余気) / 10.5日 → 乙司令(本気)。寅月 elapsed 8日 → 丙司令(中気)。経過35日 → 本気司令
2. **力量の手計算検証**: 小さい命式1件で raw スコアを手計算値と一致させる（テストコード内に計算根拠をコメントで書くこと）
3. **strength_v2**: supporters/drainers の分離が正しい（日干自身の天干1.0が除外されている）ことを検証
4. **special_pattern_suspicion**: 一五行55%以上のケースで True
5. **建禄格**: 甲日干×寅月 → 建禄格 high。月刃格: 丙日干×午月 → 月刃格 high。陰干（乙×卯月）は月刃格でなく建禄格
6. **透干判定**: 本気透干 → high、透干なし → 本気の十神で medium、比肩蔵干が格候補にならない
7. **反証**: 月支と冲する支がある命式で counter_evidence に冲の記載
8. **扶抑の分岐**: 強め域→泄耗制3候補 / 弱め域→印比2候補 / 中庸域→hold / suspicion→withheld（withheld が境界域より優先）
9. **調候**: 子月→火 medium / 午月→水 medium / 卯月→候補なし / agreement の matched・conflicted 判定
10. **統合＋後方互換**: ゴールデンケース（1990-05-15 08:30 東京・female）で `hidden_stem_command` と `pattern_candidates` の値を固定（初回実行値を表と手計算で照合してから固定）。既存キー（`structure_report.strength_index` 等）が不変であること

既存テスト（フェーズ1・2含む）を壊さないこと。
実行: `python -m pytest tests/test_shichu_structure.py tests/test_shichu_interactions.py tests/test_shichu_timing.py -q`

## 7. フォーマッタとプロンプト（最小限）

- `services/shichu_formatter.py`: サマリーに「月令：丙司令（中気）」「格局候補：正官格（高）」「推奨五行：火・土（扶抑）／火（調候）」のような1行を追加（データが無い旧出力では何も出さない）。fu_yi が hold/withheld の場合は「推奨五行：保留（境界域）」のように理由を短く出す
- w_shichu 系プロンプト6ファイル（フェーズ1で触った6つと同じ）の【四柱推命データの読み方】に追記:
  - `normalized_data.hidden_stem_command` → 月令で司令する蔵干。月支の意味を強調する根拠に使う
  - `structure_report.pattern_candidates` → 格局の**候補**。「あなたは◯◯格です」と断定せず「◯◯格の色合いが強い命式」のように候補として扱う。counter_evidence がある場合は言い切らない
  - `structure_report.yojin_candidates` → 推奨五行の候補。fu_yi(扶抑)と climate(調候)が一致する五行は開運アドバイスの軸にしてよい。status が hold/withheld のときは五行の推奨をしない
  - `strength_v2` と既存 `strength` が食い違う場合は断定を避けて幅を持たせる

## 制約・禁止事項

- 既存の出力キー・既存関数の変更禁止（追加のみ）。`_calc_strength_score()` v1 と `main_hidden_stems` は必ず温存
- 月律表・調候簡易表・建禄/月刃表・重み・閾値の独自判断による変更禁止（本書の値が正）
- 窮通宝鑑の完全表を実装しない（スコープ外）
- 外格（従格・化格・専旺格）の判定を実装しない（suspicion フラグまで）
- 計算JSONに自然言語の断定的解釈を入れない（rationale は計算根拠の記述のみ）
- 新規依存パッケージの追加禁止
- 全テスト（フェーズ1・2含む）が通ることを確認してから完了報告すること
