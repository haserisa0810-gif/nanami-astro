# タスク: 四柱推命 大運深化＋流年MVP（フェーズ2）

あなたはこのリポジトリ（nanami-astro）の実装担当です。フェーズ1で実装した相互作用エンジン（`services/shichu_interactions.py`）を大運・流年に適用し、時期鑑定用のデータを生成できるようにします。

## 前提（フェーズ1の成果物を必ず読むこと）

- `services/shichu_interactions.py` — 相互作用エンジン。柱名は任意文字列を受け付ける設計になっており、`_PILLAR_ORDER` に `daiun` / `ryunen` が既に定義済み
- `services/shichusuimei_calc.py` — `calc_shichusuimei_from_payload()` が統合点。`ten_god()` / `twelve_fortune()` / `_lichun_utc_for_year()` / `KANSHI_60` / `_sexagenary_index()` / `_calc_daiun()` を再利用する
- 設計思想は同じ: 計算事実＋タグのみ（自然言語解釈なし）、断定回避、流派差は assumptions 明記、**追加キーのみで後方互換維持**

## 成果物

1. 新規モジュール `services/shichu_timing.py` — 大運深化＋流年計算
2. `services/shichu_interactions.py` — クロススコープ検出関数の追加（既存関数は変更禁止）
3. `services/shichusuimei_calc.py` — 統合（追加キーのみ）、`engine_version` を `0.4.0` に
4. 新規テスト `tests/test_shichu_timing.py`
5. `prompts/single_web_claude_w_shichu_timing.txt` にタイミングデータの読み方を追記
6. `services/shichu_formatter.py` に現大運・今年の流年の1行表示を追加

---

## 1. クロススコープ相互作用 `detect_cross_interactions()`

`services/shichu_interactions.py` に追加:

```python
def detect_cross_interactions(
    natal_stems, natal_branches,      # 命式側: [(柱名, 干)], [(柱名, 支)]
    transit_stems, transit_branches,  # 運側:   [("daiun", 干)] など通常1件ずつ
    *, scope: str,                    # "daiun_natal" | "ryunen_natal" | "ryunen_daiun"
    hour_known: bool = True,
) -> list[dict]:
```

仕様:
- **命式内(natal×natal)・運側内(transit×transit)のペアは出さない**。必ず両グループにまたがるペアのみ検出する
- 検出対象と rule_id・タグはフェーズ1と同一（干合/支合/冲/刑/自刑/害/破）。出力 dict の `scope` フィールドに引数の scope を入れる
- **三合・方合の「完成」検出**: 命式側に2支あり運側の1支で3支が揃う場合のみ検出し、`detail` に `"completed_by": scope側の柱名`（例 `"ryunen"`）を入れる。命式内だけで完結する組は出さない（フェーズ1で検出済みのため）。半会のクロス検出（命式1支＋運1支）も対象とする
- `pillars` の整列は既存 `_PILLAR_ORDER` を使用（daiun=4, ryunen=5 で命式の後ろに並ぶ）
- `provisional` はフェーズ1と同じ: `hour_known=False` かつ関与柱に `hour` を含むとき True

## 2. 伏吟・反吟・天干冲

`services/shichu_interactions.py` に追加。

**天干冲（4組のみ。この表が正、追加禁止）**: 甲↔庚 / 乙↔辛 / 丙↔壬 / 丁↔癸（戊・己に天干冲は無い）

```python
def detect_special_kanshi_relations(
    natal_pillars,          # [(柱名, 干支2文字)] 例 [("year", "庚午"), ...]
    transit_kanshi: str,    # 運側の干支 例 "庚辰"
    transit_name: str,      # "daiun" | "ryunen"
    *, scope: str, hour_known: bool = True,
) -> list[dict]:
```

- **伏吟**: 運側の干支が命式の柱と完全一致 → `rule_id: "fukugin_v1"`, `type: "伏吟"`, tags: `["repetition", "stagnation_or_emphasis"]`
- **反吟（天剋地冲）**: 天干が天干冲 **かつ** 地支が冲 → `rule_id: "hangin_v1"`, `type: "反吟"`, tags: `["reversal", "upheaval"]`。天干冲が存在しない戊・己絡みは地支が冲でも反吟にしない
- 出力スキーマはフェーズ1の interaction と同形（`category: "kanshi"`, `members` は [命式側干支, 運側干支]、`pillars` は [命式柱名, transit_name]）

## 3. タイミング計算 `services/shichu_timing.py`

```python
def build_timing(
    p4,                      # Pillars4（shichusuimei_calc から渡す）
    daiun: dict,             # _calc_daiun() の戻り値
    *,
    birth_local: datetime,   # tz aware
    target_date: date,       # 鑑定基準日
    tz_name: str,
    hour_known: bool,
    n_ryunen: int = 3,
) -> dict:
```

### 大運の深化（daiun_detail）

`daiun["items"]`（10件）の各項目について、**既存 items は変更せず**、深化版のリストを新設する:

```python
{
    "index": 1,
    "kanshi": "庚午",
    "age_from": ..., "age_to": ..., "approx_start_year": ...,   # 既存itemsからコピー
    "stem_ten_god": "...",           # ten_god(日干, 大運天干)
    "branch_twelve_fortune": "...",  # twelve_fortune(日干, 大運地支)
    "interactions_natal": [...],     # detect_cross_interactions(scope="daiun_natal")
    "special": [...],                # detect_special_kanshi_relations(伏吟/反吟)
    "is_current": bool,              # target_date 時点の大運か
}
```

### 現大運の判定

- target_date 時点の年齢（誕生日基準の満年齢＋経過月を小数年で）を計算し、`age_from <= age < age_to` の大運を current とする
- 起運年齢前（最初の大運の age_from 未満）の場合は `current_daiun: null` とし、`note` に「起運前」を入れる

### 流年（ryunen）

- **基準年の判定は立春基準**: target_date が当年の立春（`_lichun_utc_for_year()` を再利用、ローカル時刻に変換して比較）より前なら前年を「今年」とする
- 今年から `n_ryunen` 年分（デフォルト3年）を生成:

```python
{
    "year": 2026,                    # 暦年ラベル
    "kanshi": "丙午",                # KANSHI_60[(year - 4) % 60]
    "applies_from": "lichun",        # 固定値（この干支が適用されるのは当年立春から）
    "is_current": bool,
    "stem_ten_god": "...",           # ten_god(日干, 流年天干)
    "branch_twelve_fortune": "...",  # twelve_fortune(日干, 流年地支)
    "interactions_natal": [...],     # scope="ryunen_natal"
    "interactions_daiun": [...],     # scope="ryunen_daiun"（現大運が存在する場合のみ、現大運との干合/冲など）
    "special_natal": [...],          # 命式との伏吟/反吟
    "special_daiun": [...],          # 現大運との伏吟/反吟（現大運がある場合のみ）
}
```

### 戻り値

```python
{
    "engine": "shichu_timing_v1",
    "target_date": "2026-07-14",
    "current_daiun": {...} | None,   # daiun_detail の該当項目（同じ dict）
    "daiun_detail": [...],           # 10件
    "ryunen": {"items": [...]},
    "notes": [...],                  # 起運前などの注記
}
```

## 4. `calc_shichusuimei_from_payload` への統合

- キーワード引数 `target_date: date | None = None` を**追加**（既存呼び出しを壊さないためデフォルト None → `datetime.now(ZoneInfo(tz_name)).date()`）
- 出力に追加（既存キー変更禁止）:
  - トップレベル `"timing": {...}`（build_timing の戻り値）
  - `normalized_data["timing"]` に同じものを入れる
- `input.assumptions` に追記:
  - `"ryunen_rules": "流年干支は暦年式(年-4 mod 60)。適用開始は当年立春。基準年判定も立春基準"`
  - `"timing_rules": "伏吟=干支一致、反吟=天干冲(甲庚/乙辛/丙壬/丁癸)＋地支冲の同時成立。現大運は目安年齢(簡易起運)による"`
- `engine_version` を `"0.4.0"` に

## 5. テスト `tests/test_shichu_timing.py`

pytest 形式。クロス検出・伏吟反吟はエフェメリス不要（干支直接指定）で:

1. **クロス検出がグループをまたぐペアのみ**: natal に 子・午（命式内で冲）、transit に 卯 を渡し、natal 内の子午冲が結果に**含まれない**こと、卯酉など transit 関与のみ出ることを確認
2. **三合完成**: natal 申・辰 ＋ ryunen 子 → 三合1件、`detail["completed_by"] == "ryunen"`。natal に申子辰が揃っている場合はクロス検出で三合を出さないこと
3. **伏吟**: natal 年柱 庚午 × daiun 庚午 → fukugin_v1
4. **反吟**: 甲子 × 庚午 → hangin_v1 成立／戊子 × 己午（天干冲なし）→ 不成立／甲子 × 庚子（地支冲なし）→ 不成立
5. **現大運判定**: `build_timing` に固定 target_date を渡し、`is_current` がちょうど1件、起運前の target_date では current_daiun が None
6. **立春基準の年判定**: target_date=2026-02-03（立春前）で流年の基準年が 2025（乙巳）になり、2026-02-05 で 2026（丙午）になる（エフェメリス使用可）
7. **流年干支の正当性**: 2026 → 丙午、日干庚に対する丙の十神が「偏官」
8. **統合テスト**: フェーズ1のゴールデンケース（1990-05-15 08:30 東京・female）に `target_date=date(2026, 7, 14)` を渡し、`timing.current_daiun` の kanshi・`ryunen.items[0]` の干支/十神を固定値でアサート（初回実行値を KANSHI_60 と手計算で照合してから固定すること）
9. **後方互換**: `target_date` を渡さない呼び出しが正常動作し、既存キー（`features.daiun.items` の形など）が変わっていないこと

既存テスト（`tests/test_shichu_interactions.py` 含む）を壊さないこと。
実行: `python -m pytest tests/test_shichu_timing.py tests/test_shichu_interactions.py -q`

## 6. プロンプトとフォーマッタ（最小限）

- `prompts/single_web_claude_w_shichu_timing.txt` の【四柱推命データの読み方】に追記:
  - `normalized_data.timing.current_daiun` → 今の10年運。`stem_ten_god` と `interactions_natal` を「今の時期の外圧・追い風」の根拠に使う
  - `normalized_data.timing.ryunen.items` → 今年〜数年の年運。`is_current: true` が今年。命式との冲＝変化年、支合・三合完成＝結びつき/加速の年として扱う
  - `伏吟` は「同じテーマの反復・強調」、`反吟` は「流れの反転・動揺」として翻訳し、単独で吉凶断定しない
  - 流年干支の適用は立春から（1月〜立春前は前年の干支）と注記する
- 他の w_shichu 系プロンプトには追記しない（タイミング以外の商品でノイズにしないため）
- `services/shichu_formatter.py`: サマリーに「現大運：庚午（偏官・35歳頃〜）」「今年：丙午（偏官）」のような1行を追加（timing が存在する場合のみ。存在しない旧データでは何も出さない）

## 制約・禁止事項

- 既存の出力キー・既存関数のシグネチャ変更禁止（`calc_shichusuimei_from_payload` へのキーワード引数追加のみ可）
- フェーズ1の対応表・検出ロジックの変更禁止
- 天干冲は上記4組のみ。独自判断での追加・変更禁止
- 計算JSONに自然言語の解釈文を入れない（tags のみ）
- 新規依存パッケージの追加禁止
- 全テスト（フェーズ1含む）が通ることを確認してから完了報告すること
