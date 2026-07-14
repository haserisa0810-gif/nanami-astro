# タスク: インド占星術 信頼性基盤（Vedicステップ1）

あなたはこのリポジトリ（nanami-astro）の実装担当です。`services/vedic_calc.py` に「出生時刻の確度」「月の日内安定性」「セクション別の利用可否」「再現可能な基準時」を導入します。**新しい占術技法は追加しません。** 仮の出生時刻から計算した値をAIが確定事実として読む、という現在の最大リスクを潰すフェーズです。

## 背景・合意事項（GPTレビューで確定）

- 現状の問題: 緯度経度さえあれば出生時刻の確度を見ずに ASC・ハウス・ハウス支配・ASC依存ヨーガ・D9 を計算し確定値として出力している / ダシャー基準時が `datetime.now()` 直書きで同一入力でも実行日により結果が変わる
- 出生時刻不明時は「値を見せて使うなと指示する」のではなく **ASC系セクションを出力しない**（available: false のメタデータで構造的に示す）
- 月は1日約13度動くため「時刻不明でも月基準は安定」とは限らない。**ローカル日内の安定性を実測して判定**する
- reliability の語彙は `high / medium / low / provisional / unavailable` に固定（他占術との統合層で共通）
- 追加キーは自由、**時刻既知の出生では既存出力を一切変えない**（時刻不明時のみ挙動が変わる。これは仕様修正であり合意済み）

## 成果物

1. `services/vedic_calc.py` — input_reliability / sections / as_of / calculation_context / assumptions
2. 消費者の耐性対応（`services/ai_report.py` の vedic 要約、`services/astrologer_summary.py` 等）
3. 新規テスト `tests/test_vedic_reliability.py`

---

## 1. 出生時刻確度の判定

- payload には `birth_time` キー（原文、未入力なら None）が入る（`shared.py` が付与済み。無い payload では西洋W0と同じヒューリスティック: hour が None/""/12:00仮置きの判定はせず、`birth_time` キー欠落時は `hour` が None/"" のときのみ unknown、それ以外は known とする）
- `birth_time_status`: `"known" | "unknown"`（approximate はこのリポジトリでは扱わない）

## 2. 月の日内安定性判定（時刻不明時のみ実行）

出生地タイムゾーンにおける出生日の `[00:00, 翌日00:00)` で、月のラーシ・ナクシャトラ・パダが変わるかを判定する。

**アルゴリズム（この手順で実装。月専用であり一般化しない）:**

1. ローカル日の開始 `00:00:00` と翌日 `00:00:00` を UTC に変換
2. 両端の月恒星黄経を計算し、終点が始点より小さければ +360 して連続化
3. その黄経区間に ラーシ境界(30度刻み)/ナクシャトラ境界(13°20′刻み)/パダ境界(3°20′刻み) が含まれるかを数学的に判定（カテゴリ比較でなく境界横断で判定。360度ラップの誤処理防止）
4. 中間点(12:00)でも計算し、単調増加になっているか検証。想定外なら2時間刻みの細分走査にフォールバック
5. 境界がある場合は二分探索で境界時刻を求める（精度は分単位で十分）

出力:

```python
"moon_day_stability": {
    "moon_rashi_stable_all_day": bool,
    "moon_nakshatra_stable_all_day": bool,
    "moon_pada_stable_all_day": bool,
    "boundaries": [{"kind": "nakshatra", "at_utc": "...", "from": "...", "to": "..."}],
}
```

## 3. セクション別の利用可否と出力制御

出力に追加:

```python
"input_reliability": {
    "birth_time_status": "known" | "unknown",
    "overall": "high" | "limited",
    "reasons": [...],
},
"sections": {
    "ascendant":     {"available": bool, "reliability": "...", "reasons": [...]},
    "houses":        {"available": bool, "reliability": "...", "reasons": [...]},
    "house_lords":   {"available": bool, "reliability": "...", "reasons": [...]},
    "yogas":         {"available": bool, "reliability": "...", "reasons": [...]},
    "d9":            {"available": bool, "reliability": "...", "reasons": [...]},
    "chandra_lagna": {"available": bool, "reliability": "...", "reasons": [...]},
    "dasha":         {"available": bool, "reliability": "...", "reasons": [...]},
    "gochara":       {"available": bool, "reliability": "...", "reasons": [...]},
},
```

### 時刻既知（known）

全セクション `available: true, reliability: "high"`。**既存の出力キー・値は一切変えない**（追加キーのみ）。

### 時刻不明（unknown）の制御

| セクション | 挙動 |
|---|---|
| ascendant / houses / house_lords | **計算せず出力しない**。sections で `available: false, reliability: "unavailable", reasons: ["birth_time_unknown"]` |
| ASC依存ヨーガ（house_lords を使う ダナ/ラクシュミー/ラージャ/ダルマカルマ/アマラ/ヴィパリータ/ダリドラ/シャカタ等） | 検出しない。月基準ヨーガ（ガジャケサリ/チャンドラマンガラ/ケマドルマ）のみ出力し、yogas セクションは `available: true, reliability: "provisional", reasons: ["asc_dependent_yogas_skipped"]` |
| d9 | D9 ASC は出さない。惑星のD9は出すが `reliability: "low"`（ナヴァムシャは3°20′刻みで月など速い天体は時刻で変わるため）。月のD9が日内で変わるかは moon_day_stability から判定できる範囲で reasons に記載 |
| chandra_lagna（月基準ハウス。既存出力にあれば） | 月ラーシが終日安定なら `available: true, reliability: "high"`。日内で変化するなら `reliability: "provisional"` とし、変化前後の2候補ラーシを reasons またはデータに明示 |
| dasha | 月ナクシャトラが終日安定なら `reliability: "provisional"`（開始残量は時刻依存のため high にはしない）。ナクシャトラが日内で変化する場合は `reliability: "low"` とし、両ナクシャトラそれぞれのマハー期候補を `scenarios` として出す（最大2件） |
| gochara | 月ラーシ安定なら通常出力（`reliability: "high"`。ゴチャラは出生時刻に依存しないため。ただし月からのハウス計算は月ラーシに依存する点を reasons に記載） |

## 4. as_of の一元化

- `calc_vedic_from_payload(payload, *, as_of: datetime | None = None)` にキーワード引数を追加（既存呼び出しは無変更で動く）
- 関数冒頭で一度だけ解決し（None → 現在時刻UTC）、ダシャー現在期判定・ゴチャラ等すべてこの値を使う。`datetime.now()` の散在を排除
- naive datetime は ValueError で拒否（aware のみ受け付ける）
- 出力に追加:

```python
"calculation_context": {
    "as_of_utc": "...",                 # 解決後の値
    "as_of_source": "request" | "default_now",
},
```

- 将来 `gochara_at` / `forecast_until` を追加する予定があるため、`as_of` を「現在期判定・状態判定の基準」以外の意味に流用しない

## 5. assumptions の明文化

出力に追加（挙動は変えない。宣言のみ）:

```python
"assumptions": {
    "ayanamsha": "lahiri",
    "node_mode": "mean",                          # 現実装は swe.MEAN_NODE
    "node_aspect_policy": "rahu_ketu_5_7_9",      # 現実装のまま。流派差があることの明示
    "dasha_year_basis": {"days_per_year": 365.2425, "rule_id": "vimshottari_tropical_year_v1"},
    "house_system": "whole_sign_from_asc",
    "boundary_policy": "[start,end)",
},
```

## 6. 消費者の耐性対応

時刻不明時に ASC系キー・ASC依存ヨーガが出力から消えるため、以下を grep して欠落に耐えることを確認・修正:

- `services/ai_report.py` — `_build_vedic_focus_summary` ほか vedic 参照箇所
- `services/astrologer_summary.py` — vedic themes
- `services/analyze_engine.py` / `services/services_analyze_engine.py` — structure_summary への取り込み
- プロンプトの【Vedic使用ルール】まわり: 「sections で available: false のものは言及しない」「reliability: provisional の項目は断定に使わない」を1〜2行追記（w_shichu系ではなく vedic を使うプロンプトが対象。`must_use_vedic` 関連の記述がある単独/統合プロンプトを確認）

## 7. テスト `tests/test_vedic_reliability.py`

1. **時刻既知の後方互換**: 既知の出生データで、追加キー（input_reliability/sections/calculation_context/assumptions）以外の既存キー・値が従来と一致（変更前出力との比較は「既存キーの存在と ascendant 等の主要値」で可）
2. **時刻不明の出力制御**: ascendant/houses/house_lords が出力に無い。月基準ヨーガのみ検出され、ASC依存ヨーガのslugが1つも出ない。sections の available/reliability が表の通り
3. **月の安定日**: 月がサイン中央付近にある日を選び、3層すべて stable=true
4. **月の境界日**: 出生日中に月がラーシ境界を越える日を実際に探して固定し（テスト内コメントに探し方と境界時刻を記録）、`moon_rashi_stable_all_day: false`、boundaries に境界時刻、chandra_lagna が provisional で2候補
5. **360度ラップ**: 月が魚座→牡羊座を跨ぐ日で正しく境界検出される
6. **ナクシャトラ変化とダシャー**: ナクシャトラが日内で変わる日で dasha.reliability=="low" かつ scenarios が2件
7. **as_of 再現性**: 同一入力＋固定 as_of で2回呼んで完全一致。as_of を10年後にするとマハー期が進む。naive datetime で ValueError
8. **calculation_context**: as_of 指定時 source=="request"、未指定時 "default_now"
9. **消費者 smoke**: 時刻不明の vedic 出力を ai_report の vedic 要約関数に通して例外なし

既存テストを壊さないこと。
実行: `python -m pytest tests/ -q --ignore=tests/test_external_report_prompt_safety.py --ignore=tests/test_note_article_routes.py --ignore=tests/test_payhip_payment_parsing.py --ignore=tests/test_note_article_service.py`（4ファイルは既知の環境依存で収集エラー）

## 制約・禁止事項

- 時刻既知の出生に対する既存出力の変更禁止（追加キーのみ）
- 新しい占術技法（サデサティ・タイムライン・カラカ等）の実装禁止（次フェーズ）
- ヨーガの検出ロジック・strength の変更禁止（ステップ3で扱う。今回は時刻不明時にASC依存ヨーガをスキップするだけ）
- ノードアスペクトの挙動変更禁止（assumptions での宣言のみ）
- reliability の語彙は high/medium/low/provisional/unavailable のみ使用
- 新規依存パッケージの追加禁止
- 全テストが通ることを確認してから完了報告すること
