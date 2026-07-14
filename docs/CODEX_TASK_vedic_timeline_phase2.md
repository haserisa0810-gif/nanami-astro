# タスク: インド占星術 ダシャータイムライン＋ゴチャラ月基準（Vedicステップ2）

あなたはこのリポジトリ（nanami-astro）の実装担当です。ステップ1（信頼性基盤）の上に、時期鑑定の中核である「ヴィムショッタリーの将来タイムライン」と「ゴチャラの月基準ハウス」を実装します。

**前提: ステップ1が実装済み**（`input_reliability` / `sections` / `as_of` / `calculation_context` / `moon_day_stability` が `services/vedic_calc.py` に存在すること）。

## 背景・合意事項

- 現状のダシャーは「現在のマハー期＋アンタラ期」のみ。時期鑑定には将来の切替タイムラインが必要
- 年換算は 365.2425日を維持（`assumptions.dasha_year_basis` 通り）。境界は `[start, end)`
- 各期の支配星は「名前だけ」でなく**ネイタルでの状態（品位・在住・支配）を根拠として添付**する（AIが「金星期=恋愛好調」のような紋切り型でなく、その人の金星の状態から読めるように）
- ゴチャラには月からのハウス（チャンドララグナ基準）を併記。時刻不明時でも月基準は使える（ステップ1の安定性判定が前提）
- `as_of` は現在期判定の基準、`gochara_at` はトランジット評価日時。**gochara_at を現在ダシャー判定に流用しない**（GPT合意）
- サデサティ・ヨーガ品質改修はステップ3。今回は実装しない
- 追加キーのみで後方互換維持。既存の `dasha`（maha/antara現在期）は変更しない

## 成果物

1. `services/vedic_calc.py` — `dasha_timeline` / ゴチャラの `house_from_moon` / `gochara_at` パラメータ
2. `services/ai_report.py` — vedic 要約への「次の切替時期」1行追加
3. 新規テスト `tests/test_vedic_timeline.py`

---

## 1. ダシャータイムライン `dasha_timeline`

`calc_vedic_from_payload` に `forecast_years: int = 10` パラメータを追加し、出力に追加:

```python
"dasha_timeline": {
    "rule_id": "vimshottari_timeline_v1",
    "as_of_utc": "...",
    "forecast_until_utc": "...",          # as_of + forecast_years（365.2425日換算）
    "periods": [                           # ウィンドウ [as_of, forecast_until) に重なるマハー期のみ
        {
            "level": "maha",
            "lord": "Venus",
            "start_utc": "...", "end_utc": "...",   # [start, end)
            "is_current": true,                      # as_of がこの区間にあるか
            "lord_natal": { ... },                   # 下記
            "antara": [                              # このマハー内でウィンドウに重なるアンタラ期
                {"lord": "Sun", "start_utc": "...", "end_utc": "...", "is_current": false},
                ...
            ],
        },
        ...
    ],
}
```

実装規則:
- 既存の `_dasha_chain_from_moon` と同じ起点計算（月ナクシャトラの経過割合）を使い、**マハー周期列を出生から連続生成**してウィンドウに重なるものを抽出する。ロジックは既存関数から抽出・共通化してよいが、既存 `dasha` 出力（maha/antara/active_themes/reliability/scenarios）は変えない
- アンタラの長さ: マハー期間 × (アンタラ支配星の年数 / 120)。マハー支配星から始まる9期で順繰り
- `is_current` はマハーで1件、その中のアンタラで1件だけ true（as_of がウィンドウ内にある前提）
- 区間はすべて `[start, end)`。`end_utc == 次の start_utc`（連続、隙間・重複なし）

### lord_natal（各期支配星のネイタル根拠）

```python
"lord_natal": {
    "rashi": "...",
    "nakshatra": "...",
    "retrograde": bool,
    "dignity": {...},              # 既存 _build_dignity の出力（惑星に既にあるものを参照）
    "house_no": int | None,        # 出生時刻既知のときのみ。不明なら None
    "ruled_houses": [int] | None,  # 支配するハウス番号。時刻既知のみ。Rahu/Ketu は常に None
}
```

- 出生時刻不明のとき `house_no` / `ruled_houses` は None（ステップ1でASC系を出さない方針と整合）
- Rahu/Ketu は品位辞書に無い項目があれば安全に None/省略とし、例外を出さない

### 信頼性との統合

- `sections` に `"dasha_timeline"` を追加。reliability は `sections["dasha"]` と同値（時刻既知=high / ナクシャトラ安定=provisional / 不安定=low）
- ナクシャトラ不安定（low）の場合もタイムラインは**主計算の月位置で1本だけ**出す（2本併記はJSON肥大とAI混乱のため不採用。scenarios が既にマハー候補を示している）

## 2. ゴチャラの月基準ハウス

- `build_vedic_gochara` に natal 月ラーシ番号（および時刻既知なら ASC ラーシ番号）を渡せるようにし、各惑星行に追加:
  - `"house_from_moon"`: natal 月ラーシを第1室とした whole sign ハウス番号
  - `"house_from_asc"`: 時刻既知のときのみ。不明なら出力しない
- 既存の行キーは変更しない（追加のみ）。`build_vedic_gochara` の既存シグネチャ互換を保つ（新引数はデフォルト None で、None なら従来出力）

## 3. `gochara_at` パラメータ

- `calc_vedic_from_payload(payload, *, as_of=None, gochara_at=None, forecast_years=10)` に追加
- ゴチャラ評価日時の優先順位: `gochara_at` 引数 > payload の `gochara_date` > payload の `target_date` > `as_of`
- `gochara_at` は aware datetime または date を受け付け、naive datetime は ValueError
- **`gochara_at` を現在ダシャー判定・`is_current` 判定に使わない**（それらは常に as_of 基準）
- `calculation_context` に `"gochara_at_utc"` と `"forecast_until_utc"` を追加

## 4. AI要約への反映（最小限）

`services/ai_report.py` の `_build_vedic_focus_summary` に、`dasha_timeline` がある場合のみ1行追加:
「現在は◯◯期（△△アンタラ）。次の切替は YYYY年M月頃に□□期へ。」
- 時刻不明（reliability が provisional/low）の場合は「（出生時刻不明のため暫定）」を付ける（既存の暫定注記パターンに合わせる）

## 5. テスト `tests/test_vedic_timeline.py`

日付そのものの正しさは「不変条件」で検証する（外部の暦との突き合わせが不要な設計にする）:

1. **連続性**: 全マハー期が隙間・重複なく連続（`end_utc == 次のstart_utc`）。アンタラも各マハー内で連続し、最初のアンタラ start == マハー start、最後のアンタラ end == マハー end（ウィンドウで切り取られた期を除く。切り取りの扱いをテスト内コメントで明記）
2. **期間比率**: 完全に含まれるマハー期の長さが `VIMSHOTTARI_YEARS[lord] * 365.2425` 日と一致（±1秒）。アンタラ長がマハー長 × 年数/120 と一致
3. **順序**: マハーの lord 列が `VIMSHOTTARI_SEQUENCE` の順序に従う
4. **唯一の現在期**: is_current がマハーで1件・その中のアンタラで1件。既存 `dasha["maha"]["lord"]` と timeline の current maha lord が一致（同じ起点計算の検証）
5. **[start,end) 境界**: as_of をあるマハー期の `end_utc` ちょうどに設定すると、次の期が is_current になる
6. **ウィンドウ**: forecast_years=1 と 10 で periods 数が変わり、全期がウィンドウに重なる
7. **lord_natal**: 時刻既知で house_no/ruled_houses が入る。時刻不明で両方 None。Rahu または Ketu がタイムラインに含まれるケースで ruled_houses が None（含まれない場合は直接 lord_natal 構築関数を呼んで検証）
8. **ゴチャラ月基準**: 月ラーシ番号を固定して house_from_moon を手計算値と照合（例: natal 月=牡牛(2) でトランジット惑星が獅子(5) → 4室）。時刻不明で house_from_asc が無いこと
9. **gochara_at の分離**: gochara_at を10年後にしてもダシャーの is_current が as_of 基準のまま変わらないこと。naive datetime で ValueError
10. **ゴールデン**: 固定出生データ＋固定 as_of で timeline 全体を2回計算し完全一致。current maha/antara の lord と切替日を固定値でアサート（初回実行値を上記不変条件で検証してから固定）
11. **後方互換**: 既存キー（dasha.maha/antara、gochara の既存行キー、sections 等）が不変

既存テスト（test_vedic_reliability.py 含む）を壊さないこと。
実行: `python -m pytest tests/ -q --ignore=tests/test_external_report_prompt_safety.py --ignore=tests/test_note_article_routes.py --ignore=tests/test_payhip_payment_parsing.py --ignore=tests/test_note_article_service.py`

## 制約・禁止事項

- 既存の `dasha` 出力（maha/antara/active_themes/reliability/scenarios）の変更禁止
- サデサティ・ヨーガ関連の変更禁止（ステップ3）
- プラティアンタラ（孫期）の実装禁止（スコープ外）
- 年換算 365.2425日・`[start,end)` 境界の変更禁止
- 時刻不明時に house_no / ruled_houses / house_from_asc を出さない（ステップ1の方針と整合）
- 新規依存パッケージの追加禁止
- 全テストが通ることを確認してから完了報告すること
