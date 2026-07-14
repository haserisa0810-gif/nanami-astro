# タスク: 西洋占星術 計算基盤の整備（フェーズW0）

あなたはこのリポジトリ（nanami-astro）の実装担当です。西洋占星術の深化に先立ち、計算のデータモデルを整備します。**このフェーズは新しい占術機能を足すものではなく、既存出力に「速度・接近/分離・ルール明示・データ品質」を追加キーで足すものです。**

## 前提（必ず読むこと）

- `services/western_calc.py` — `calc_western_from_payload()` / `calc_aspects()` / `ASPECTS` / `ORB` / `angle_diff()`。**swe.calc_ut の戻り値 xx[3] が経度速度（度/日）**だが、現状は逆行フラグ判定に使った後に捨てている
- `services/transit_calc.py` — `_calc_today_planets()` / `_match_aspects()`
- 設計思想（四柱側で確立済み・踏襲する）: 事実＋タグのみ、断定回避、採用ルールは assumptions/rules に明記、**追加キーのみで後方互換維持**
- 既存の消費者（`services/option_recommendation.py`, `services/api_calc.py` 等）は `aspects` の `planet1/planet2/type/orb` と `planets` の `name/lon/sign/house/retrograde` に依存している。**これらのキー・値・検出されるアスペクトの集合を一切変えないこと**（orb 値の変更も禁止）

## 成果物

1. `services/western_calc.py` — 速度保存・アスペクト項目の拡張・calculation_rules・data_quality・ノード軸フラグ
2. `services/transit_calc.py` — 当日天体の速度保存・トランジット×ネイタルの applying 判定
3. 新規テスト `tests/test_western_w0.py`
4. プロンプト・フォーマッタの変更は**しない**（次フェーズで実施）

---

## 1. 天体の速度保存（western_calc.py）

- 各 planet dict に追加キー:
  - `"speed"`: 経度速度（度/日、float、小数4桁で丸め）。swe.calc_ut の xx[3]
  - ハウスカスプ・ASC/MC などの角度点には speed を付けない（または `None`）
- 既存の `retrograde` フラグは変更しない（`speed < 0` と一致するはずだが、既存ロジックを触らない）

## 2. アスペクト項目の拡張（calc_aspects）

既存の各アスペクト dict（`planet1/planet2/type/orb`）に以下を**追加**する:

- `"exact_angle"`: アスペクトの正確な角度（0/60/90/120/180）
- `"actual_angle"`: 実際の離角 `angle_diff(lon1, lon2)`（0〜180、小数2桁）
- `"signed_deviation"`: `actual_angle - exact_angle`（正=正確な角度より広い、小数2桁）
- `"orb_limit"`: そのアスペクト種の許容orb（既存 ORB 辞書の値）
- `"applying"`: 接近中なら true、分離中なら false、判定不能（速度がない）なら null

### applying/separating の判定式（この式の通りに実装すること）

```
signed_sep = ((lon1 - lon2 + 180) % 360) - 180       # -180..180
d_abs      = abs(signed_sep)                          # 0..180 = actual_angle
deviation  = d_abs - exact_angle                      # signed_deviation

# d_abs の時間変化率:
#   signed_sep > 0 のとき rate = speed1 - speed2
#   signed_sep < 0 のとき rate = -(speed1 - speed2)
#   signed_sep == 0 のとき rate = abs(speed1 - speed2)（合の離れ始め＝分離扱い）
rate = (speed1 - speed2) if signed_sep > 0 else -(speed1 - speed2) if signed_sep < 0 else abs(speed1 - speed2)

# deviation が 0（イグザクト）に向かっていれば接近:
applying = (deviation > 0 and rate < 0) or (deviation < 0 and rate > 0)
# deviation == 0 は「イグザクト」: rate != 0 なら分離が始まる直前なので applying = False
# speed1 または speed2 が取得できない場合は applying = None
```

**手計算検証例（テストに使うこと）**:
- A: lon=10, speed=+1.0 / B: lon=101, speed=0.0 → signed_sep=-91, d_abs=91, square(90) の deviation=+1.0, rate=-(1.0-0.0)=-1.0 → deviation>0 かつ rate<0 → **applying=True**
- A: lon=10, speed=0.0 / B: lon=101, speed=+1.0 → rate=-(0.0-1.0)=+1.0 → deviation>0 かつ rate>0 → **applying=False**
- speed が両方 None → **applying=None**

`calc_aspects` は planet dict に `"speed"` キーがあれば使い、なければ `applying=None` とする（シグネチャ変更禁止、既存呼び出しを壊さない）。

## 3. ノード軸の重複フラグ

North Node と South Node は同一軸のため、両方をアスペクト計算に入れると同じイベントが二重計上される（例: X-NorthNode の合 と X-SouthNode の衝 は同じ事実）。

- **検出集合は変えない**。South Node が関与する各アスペクト dict に `"axis_mirror": true` を追加するのみ（North Node 側には付けない）
- 後段（AI・集計layer）がこのフラグで重複を除外できるようにする

## 4. calculation_rules と data_quality（calc_western_from_payload の出力に追加）

トップレベルに追加キー:

```python
"calculation_rules": {
    "rule_set_id": "western_default_v1",
    "aspects": {"conjunction": {"angle": 0, "orb": 8}, ...},  # 既存 ASPECTS/ORB を機械的に展開
    "house_system": <採用ハウスシステム>,
    "node_mode": <採用ノードモード>,
    "lilith_mode": <採用リリスモード>,
    "notes": ["アスペクト対象・orbは従来実装と同一（後方互換）", "South Node関与のアスペクトは axis_mirror=true でマーク"],
},
"data_quality": {
    "birth_time": "known" | "unknown",     # payload の時刻が未入力・不明なら unknown
    "houses_available": bool,               # ハウス計算が実座標・実時刻に基づくか
    "coordinates": "provided" | "default",  # 緯度経度が入力されたか既定値か
},
"engine_version_western": "w0.1.0",
```

- 出生時刻・座標の「未入力」をどう表現しているかは `calc_western_from_payload` の payload 処理と呼び出し元（`services/analyze_request_service.py` の payload 構築）を読んで判断し、判定根拠をコード内コメントでなく `data_quality` の値として正しく出すこと
- 判別不能な場合は安全側（unknown / default）に倒す

## 5. transit_calc.py への適用

- `_calc_today_planets()` の各天体 dict に `"speed"` を追加
- `_match_aspects()` の各ヒット dict に `"applying"` を追加。**ネイタル天体の速度は 0 として扱う**（ネイタルは固定点。式は §2 と同一で speed2=0）
- `exact_angle` / `signed_deviation` / `orb_limit` も同様に追加
- 既存キー（transit_planet/natal_planet/aspect/orb/…）は不変

## 6. テスト `tests/test_western_w0.py`

pytest 形式。applying の式検証は速度を直接指定した合成データで行う（エフェメリス不要）:

1. **applying 式**: §2 の手計算例3ケースをそのまま検証（True / False / None）
2. **signed_deviation**: `abs(signed_deviation) == orb`（誤差1e-6以内）が全アスペクトで成立
3. **合成データでの境界**: signed_sep が負のケース・正のケース両方で applying が対称に判定される
4. **実データ smoke**（ephe/ 使用可）: 適当な出生データで `calc_western_from_payload` を呼び、(a) 全 planet に speed が入る、(b) `retrograde == (speed < 0)` が全天体で一致、(c) aspects の全項目に新フィールドが存在、(d) calculation_rules / data_quality / engine_version_western が存在
5. **ノード軸フラグ**: South Node 関与のアスペクトのみ `axis_mirror` が true
6. **data_quality**: 時刻未入力の payload で `birth_time == "unknown"`
7. **後方互換**: 既存キー（planets の name/lon/sign/house/retrograde、aspects の planet1/planet2/type/orb）が全て残っており、**検出されるアスペクトの (planet1, planet2, type) 集合が変更前と同一**（変更前の関数をコピーして比較する必要はなく、既存テスト＋アスペクト数の妥当性確認で可）
8. **トランジット**: `_match_aspects` に速度付きトランジット天体を渡し、applying が期待通りになる合成ケース1件

既存テスト（四柱側含む）を壊さないこと。
実行: `python -m pytest tests/ -q`（収集エラーになる既存4ファイルは無視してよい: test_external_report_prompt_safety / test_note_article_routes / test_payhip_payment_parsing / test_note_article_service）

## 制約・禁止事項

- 既存キーの削除・改名・値変更の禁止（追加のみ）
- 検出されるアスペクトの集合・orb値・対象天体の変更禁止（South Node の除外もしない。フラグ付与のみ）
- 関数シグネチャの変更禁止
- プロンプト・フォーマッタ・ルート層の変更禁止（このフェーズは計算層のみ）
- 新規依存パッケージの追加禁止
- 全テストが通ることを確認してから完了報告すること
