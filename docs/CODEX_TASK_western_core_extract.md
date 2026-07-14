# タスクA: 西洋計算コアの抽出と phase 三値化（W0同期・正本側）

あなたはこのリポジトリ（nanami-astro）の実装担当です。姉妹リポジトリ nanami-products と西洋計算コアを「コピー同期」で共有する体制を作ります。**nanami-astro が正本**であり、同期対象ファイルはこのタスクで作ります（products 側への同期はタスクBで別途行うので、このタスクでは nanami-astro のみ変更する）。

## 背景・合意事項

- 同期の単位は「両リポで完全に同一なファイル」。そのため純粋な計算コアを専用モジュールに抽出する
- 計算コアに商品固有の条件分岐を持ち込まない（geometry・アスペクト・ルール定義のみ）
- アスペクトの接近/分離は boolean `applying` に加えて三値以上の `phase` を正規表現とする（両リポ共通の意味論）
- 追加キーのみで後方互換維持。既存テストを壊さない

## 成果物

1. 新規 `services/western_core.py` — 同期対象となる計算コア
2. `services/western_calc.py` — コアを import して再エクスポート（動作不変）
3. 新規 `tests/fixtures/western_core_golden.json` — エフェメリス不要のゴールデンfixture
4. 新規 `tests/test_western_core_golden.py` — fixture 検証テスト（これも同期対象）
5. 既存テストの必要最小限の更新

---

## 1. `services/western_core.py`

`services/western_calc.py` から以下を**移動**し、western_calc.py 側は `from services.western_core import ...` で再エクスポートする（既存の import 元を壊さない）:

- 定数: `ASPECTS`, `ORB`
- 関数: `norm360`, `sign_of`, `house_of`, `angle_diff`, `aspect_applying`, `calc_aspects`

さらにコアに**新規追加**:

```python
WESTERN_CORE_VERSION = "2026.07.14-w0"   # コア変更のたびに日付で更新する
EXACT_THRESHOLD_DEG = 0.05               # phase=exact の判定閾値（丸め前の偏差に適用）

def aspect_phase(deviation: float, applying: bool | None) -> str:
    """applying判定と丸め前偏差から三値phaseを返す。"""
    # 戻り値: "applying" | "separating" | "exact" | "unknown"
```

判定規則:
- 速度不明（applying が None 相当）→ `"unknown"`
- `abs(deviation) <= EXACT_THRESHOLD_DEG`（**丸め前**の値で判定）→ `"exact"`
- それ以外は applying True → `"applying"` / False → `"separating"`

`calc_aspects` の各アスペクト dict に追加:
- `"phase"`: 上記の三値＋unknown
- **exact のとき `"applying"` は `None` にする**（true/false に押し込むと意味が曖昧になるため。両リポ合意事項）
- 既存の `applying` の意味変更はこの1点のみ（|deviation| <= 0.05 のケース。既存テストでこの領域を踏むものがあれば期待値を更新してよい）

`build_calculation_rules()` ヘルパーもコアに置く:

```python
def build_calculation_rules(house_system: str, node_mode: str, lilith_mode: str, notes: list[str]) -> dict:
    # 既存の calculation_rules 生成を関数化し、以下を追加:
    #   "core_version": WESTERN_CORE_VERSION,
    #   "exact_threshold_deg": EXACT_THRESHOLD_DEG,
```

`calc_western_from_payload` はこのヘルパーを使うよう変更（出力は既存キー＋上記2キー追加のみ）。

**コアの先頭に同期ヘッダコメントを入れる**:

```python
"""西洋占星術の計算コア。

このファイルは nanami-astro が正本で、nanami-products へ
scripts/sync_western_core.py（products側）でコピー同期される。
編集は必ず nanami-astro 側で行い、商品固有のロジックを入れないこと。
"""
```

## 2. ゴールデンfixture `tests/fixtures/western_core_golden.json`

**エフェメリス不要**（合成の経度・速度を直接指定）で、両リポで同一実行できる形式:

```json
{
  "core_version": "2026.07.14-w0",
  "cases": [
    {
      "name": "applying_square",
      "planets": [
        {"name": "A", "lon": 10.0, "speed": 1.0},
        {"name": "B", "lon": 101.0, "speed": 0.0}
      ],
      "expected_aspects": [
        {"planet1": "A", "planet2": "B", "type": "square", "orb": 1.0,
         "signed_deviation": 1.0, "applying": true, "phase": "applying"}
      ]
    }
  ]
}
```

ケースは最低8つ: applying / separating / 速度不明(unknown) / exact（偏差0.0と0.04の2件、applying=None を確認）/ 偏差0.06でexactにならない境界 / 合(0度)と衝(180度)の折返し近傍 / 複数アスペクト同時成立。期待値はすべて手計算で検証してからfixtureに固定すること。

## 3. テスト `tests/test_western_core_golden.py`

- fixture を読み、各ケースで `calc_aspects` を実行し、expected_aspects の各フィールドが一致することを検証（期待値にないフィールドは比較しない）
- `WESTERN_CORE_VERSION == fixture["core_version"]` を検証（コア変更時に fixture 更新を強制する仕掛け）
- **このテストファイルは products にもそのままコピーされる前提**なので、nanami-astro 固有の import（shichu等）を入れない。`services.western_core` と fixture のみに依存させる

## 4. 既存コード・テストの整合

- `tests/test_western_w0.py` に phase の検証を追加: applying=True のケースで `phase == "applying"`、None のケースで `"unknown"`
- `services/transit_calc.py` のトランジットヒットにも `"phase"` を追加（`aspect_phase` を利用、ネイタル速度0の規約は既存のまま）
- 既存の全テストが通ること（applying の exact 領域変更で落ちるものだけ期待値更新可）

実行: `python -m pytest tests/ -q`

## 制約・禁止事項

- 既存キーの削除・改名禁止（applying の exact 時 None 化のみ例外として許可）
- 検出されるアスペクトの集合・orb 値の変更禁止
- western_core.py に商品固有ロジック・リポジトリ固有 import を入れない（標準ライブラリのみ。swisseph も import しない — 純粋な幾何計算に限定）
- 新規依存パッケージの追加禁止
- 全テストが通ることを確認してから完了報告すること
