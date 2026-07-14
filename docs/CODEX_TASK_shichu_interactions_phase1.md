# タスク: 四柱推命 相互作用エンジン＋神殺の実装（フェーズ1）

あなたはこのリポジトリ（nanami-astro）の実装担当です。以下の仕様で四柱推命の計算エンジンを拡張してください。

## 背景と設計思想

- 計算結果は AI（Claude）に JSON で渡され、AI が鑑定文を生成する。**計算JSONには自然言語の解釈文を入れない**。事実＋タグのみ。
- 既存コード `services/shichusuimei_calc.py` の設計思想を踏襲する:
  - 計算根拠を breakdown / debug に残す
  - 断定ラベルを避ける
  - 流派差のある判定は assumptions に採用ルールを明記する
- **後方互換必須**: 既存の出力キーの意味・値を一切変えない。新機能は追加キーのみで実装する。
- 依存ライブラリの追加は禁止（標準ライブラリ＋既存の pyswisseph のみ）。

## 成果物

1. 新規モジュール `services/shichu_interactions.py` — 相互作用エンジン＋神殺
2. `services/shichusuimei_calc.py` — 上記を呼び出して出力に統合（追加キーのみ）、`engine_version` を `0.3.0` に更新
3. 新規テスト `tests/test_shichu_interactions.py`
4. `services/shichu_formatter.py` — 相互作用・神殺の簡易表示を追加
5. `prompts/single_web_claude_w_shichu.txt` の【四柱推命データの読み方】に新キーの読み方を追記（他の w_shichu 系プロンプトにも同じ追記）

---

## 1. 相互作用エンジン `services/shichu_interactions.py`

### 設計要件

- 入力は「(柱名, 干) のリスト」「(柱名, 支) のリスト」を受け取る汎用関数とする。柱名は `"year" / "month" / "day" / "hour"` だが、**将来 `"daiun" / "ryunen"` を渡せる設計にする**（柱名を固定の4値に限定しない）。
- 各検出結果は次のスキーマの dict で返す:

```python
{
    "rule_id": "branch_clash_v1",       # ルール識別子（種類ごとに固定文字列＋バージョン）
    "type": "冲",                        # 冲/支合/三合/半会/方合/刑/自刑/害/破/干合
    "category": "branch",               # "stem" | "branch"
    "scope": "natal",                    # フェーズ1では常に "natal"
    "pillars": ["month", "day"],        # 関与する柱（検出順ではなく year→month→day→hour の順で整列）
    "members": ["午", "子"],             # 関与する干支の文字
    "status": "detected",               # フェーズ1では "detected" 固定（下記「合化の扱い」参照）
    "provisional": False,               # 時柱が関与し、かつ出生時刻が不明のとき True
    "tags": ["tension", "change"],     # 解釈タグ（下記タグ表から）
    "detail": {}                        # 種類固有の補足（三合なら "element": "水" など）
}
```

### 合化の扱い（重要）

- 干合・支合・三合・方合は**「組み合わせの検出」のみ**を行い、`status: "detected"` とする。
- 五行への変化（化）の成立判定はフェーズ1では**実装しない**。`detail` に `"transformation": "not_evaluated"` を入れ、assumptions に「合化の成立判定は未実装（検出のみ）」と明記する。
- 三合は3支揃いを `type: "三合"`、2支のみ（生支＋旺支、または旺支＋墓支の組）を `type: "半会"` として区別する。旺支を含まない2支（生支＋墓支）は検出しない。

### 対応表（この表の通りに実装すること。改変・補完禁止）

**干合（5組）** — `rule_id: "stem_combination_v1"`, tags: `["bonding", "attraction"]`
| 組 | detail.element（参考値として格納のみ） |
|---|---|
| 甲＋己 | 土 |
| 乙＋庚 | 金 |
| 丙＋辛 | 水 |
| 丁＋壬 | 木 |
| 戊＋癸 | 火 |

**支合／六合（6組）** — `rule_id: "branch_harmony_v1"`, tags: `["bonding", "stability"]`
子＋丑 / 寅＋亥 / 卯＋戌 / 辰＋酉 / 巳＋申 / 午＋未

**冲（6組）** — `rule_id: "branch_clash_v1"`, tags: `["tension", "change"]`
子↔午 / 丑↔未 / 寅↔申 / 卯↔酉 / 辰↔戌 / 巳↔亥

**三合（4局）** — `rule_id: "branch_trine_v1"`（三合）/ `"branch_half_trine_v1"`（半会）, tags: `["alliance", "amplification"]`
| 局 | 生・旺・墓 | detail.element |
|---|---|---|
| 水局 | 申・子・辰 | 水 |
| 火局 | 寅・午・戌 | 火 |
| 金局 | 巳・酉・丑 | 金 |
| 木局 | 亥・卯・未 | 木 |

**方合（4組）** — `rule_id: "branch_directional_v1"`, tags: `["alliance", "seasonal_force"]`（3支揃いのみ検出）
| 組 | detail.element |
|---|---|
| 寅・卯・辰 | 木 |
| 巳・午・未 | 火 |
| 申・酉・戌 | 金 |
| 亥・子・丑 | 水 |

**刑** — `rule_id: "branch_punishment_v1"`, tags: `["friction", "self_undermining"]`
- 三刑（2支ペアでも検出し、3支揃いは detail に `"complete": true`）: 寅→巳、巳→申、申→寅（無恩の刑）／丑→戌、戌→未、未→丑（恃勢の刑）
- 相刑: 子↔卯
- 自刑（同じ支が2つ）: 辰・午・酉・亥 — `type: "自刑"`, `rule_id: "branch_self_punishment_v1"`

**害（6組）** — `rule_id: "branch_harm_v1"`, tags: `["undermining", "interpersonal_strain"]`
子↔未 / 丑↔午 / 寅↔巳 / 卯↔辰 / 申↔亥 / 酉↔戌

**破（6組）** — `rule_id: "branch_destruction_v1"`, tags: `["disruption"]`
子↔酉 / 丑↔辰 / 寅↔亥 / 卯↔午 / 巳↔申 / 戌↔未

### 検出仕様の細則

- ペア系（干合・支合・冲・刑・害・破）は柱の全組み合わせ C(4,2)=6 を走査。同じペアが複数回成立する場合（例: 午が2つと子1つ）は組み合わせごとに1件ずつ出す。
- 同一柱内（干と支の間）の判定はしない。干合は天干同士、支系は地支同士のみ。
- 3支系（三合・方合）は組み合わせ C(4,3) と全体から判定し、重複出力しない（3支揃いが成立したらその部分集合の半会は出さない）。

## 2. 神殺（6種に限定）

`services/shichu_interactions.py` 内に実装。出力スキーマ:

```python
{
    "name": "天乙貴人",
    "rule_id": "tenotsu_kijin_v1",
    "basis": "day_stem",            # 判定の基準（day_stem / day_branch / year_branch / day_pillar）
    "found_in": ["month"],          # 該当した柱
    "members": ["丑"],
    "provisional": False,
    "tags": ["support", "protection"]
}
```

### 対応表（この通りに実装。流派差は assumptions に記載）

**天乙貴人** — 日干基準、全地支を走査。tags: `["support", "protection"]`
| 日干 | 貴人の支 |
|---|---|
| 甲・戊・庚 | 丑・未 |
| 乙・己 | 子・申 |
| 丙・丁 | 亥・酉 |
| 壬・癸 | 巳・卯 |
| 辛 | 午・寅 |

**駅馬** — 日支基準（三合グループ）、全地支を走査。tags: `["movement", "travel", "restlessness"]`
| 日支 | 駅馬 |
|---|---|
| 申・子・辰 | 寅 |
| 寅・午・戌 | 申 |
| 巳・酉・丑 | 亥 |
| 亥・卯・未 | 巳 |

**咸池（桃花）** — 日支基準。tags: `["charm", "attraction"]`
| 日支 | 咸池 |
|---|---|
| 申・子・辰 | 酉 |
| 寅・午・戌 | 卯 |
| 巳・酉・丑 | 午 |
| 亥・卯・未 | 子 |

**華蓋** — 日支基準。tags: `["solitude", "spirituality", "artistry"]`
| 日支 | 華蓋 |
|---|---|
| 申・子・辰 | 辰 |
| 寅・午・戌 | 戌 |
| 巳・酉・丑 | 丑 |
| 亥・卯・未 | 未 |

**羊刃** — 日干基準・**陽干のみ採用**（陰干の羊刃は流派差が大きいため不採用。assumptions に明記）。tags: `["intensity", "sharpness"]`
| 日干 | 羊刃の支 |
|---|---|
| 甲 | 卯 |
| 丙 | 午 |
| 戊 | 午 |
| 庚 | 酉 |
| 壬 | 子 |

**魁罡** — 日柱が次の4干支のいずれか: 庚辰・庚戌・壬辰・戊戌。`basis: "day_pillar"`, `found_in: ["day"]`。tags: `["strong_will", "extremes"]`

- 駅馬・咸池・華蓋は年支基準の流派もあるが、**日支基準のみ採用**（assumptions に明記）。
- 神殺の判定対象は自分自身の柱を除く…ではなく**全柱を走査**（日支自身が該当する場合も含める）。

## 3. `calc_shichusuimei_from_payload` への統合

- 出生時刻の確実性を判定: `payload.get("hour")` が `None / "" / "不明"` なら `hour_known = False`。
- 出力に以下を**追加**（既存キーは変更禁止）:

```python
"time_certainty": {
    "hour_known": bool,
    "policy": "hour柱・時柱関与の判定は provisional=true でマークする",
},
"interactions": {
    "engine": "shichu_interactions_v1",
    "scope": "natal",
    "items": [...],           # 相互作用の検出結果リスト
},
"shinsatsu": {
    "rules_version": "v1",
    "items": [...],
},
```

- 同じ内容を `normalized_data["interactions"]` / `normalized_data["shinsatsu"]` にも入れる（プロンプトは normalized_data を参照するため）。
- `hour_known == False` のとき、時柱（`"hour"`）が関与する interaction / 神殺の `provisional` を True にする。
- `input.assumptions` に追記:
  - `"interaction_rules": "干合/支合/三合/半会/方合/冲/刑/自刑/害/破を検出。合化の成立判定は未実装（検出のみ）"`
  - `"shinsatsu_rules": "天乙貴人(日干)/駅馬(日支)/咸池(日支)/華蓋(日支)/羊刃(陽干のみ)/魁罡の6種。年支基準は不採用"`
- `engine_version` を `"0.3.0"` に更新。

## 4. テスト `tests/test_shichu_interactions.py`

pytest 形式。**エフェメリス不要にするため、干支を直接指定して検証する**（`Pillars4` を直接構築、またはエンジン関数に干支リストを渡す）。最低限:

1. **冲**: 日支「子」×月支「午」で branch_clash が1件、pillars が `["month", "day"]` 順で出る
2. **支合**: 子＋丑の検出
3. **三合と半会の区別**: 申・子・辰の3支で三合1件（半会は出ない）／申・子のみで半会1件／申・辰のみ（旺支なし）は0件
4. **自刑**: 午が2柱にある場合に自刑1件
5. **害・破・干合**: 各1ケース
6. **神殺**: 日干「甲」で丑・未が天乙貴人になる／日柱「庚辰」で魁罡／日干「乙」（陰干）で羊刃が出ない
7. **provisional**: hour_known=False のとき時柱関与の項目だけ provisional=True になる（時柱を含む冲などで確認）
8. **後方互換**: `calc_shichusuimei_from_payload` の既存キー（`pillars` / `summary` / `features` / `normalized_data.pillars` など）が引き続き存在する（実出生データ1件で smoke テスト。エフェメリスは `ephe/` にあるものを使用）
9. **ゴールデンケース**: 実出生データ1件（例: 1990-05-15 08:30 東京・female）で四柱・interactions・shinsatsu の計算事実を固定値でアサート（初回実行値を確認の上で固定してよいが、干支表と目視照合すること）

既存テストを壊さないこと。実行: `python -m pytest tests/ -x -q`

## 5. フォーマッタとプロンプト（最小限）

- `services/shichu_formatter.py`: `extract_shichu_data` に `interactions` / `shinsatsu` を追加し、`render_shichu_summary_html` に「命式内の作用: 午未 支合・子午 冲」「神殺: 天乙貴人(月支)」のような1行表示を追加（provisional の項目には「※時刻未確定」を付ける）。
- `prompts/single_web_claude_w_shichu.txt` ほか w_shichu 系プロンプトの【四柱推命データの読み方】に追記:
  - `normalized_data.interactions.items` → 柱同士の作用（冲=緊張・変化、支合=結びつき等）。**status が detected は「組み合わせがある」事実のみで、吉凶断定に使わない**
  - `normalized_data.shinsatsu.items` → 神殺。**単独で強い予言・吉凶断定に使わず、他の根拠と重なるときだけ言及する**
  - `provisional: true` の項目は「出生時刻が確定すれば」と留保をつける

## 制約・禁止事項

- 既存の出力キー・関数シグネチャの変更禁止（追加のみ）
- 対応表の独自判断による変更・追加禁止（上記の表が正）
- 計算JSONに自然言語の解釈文を入れない（tags のみ）
- 新規依存パッケージの追加禁止
- 全テストが通ることを確認してから完了報告すること
