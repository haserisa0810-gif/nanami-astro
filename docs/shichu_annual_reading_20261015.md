# 四柱推命の年運と占い師用分析の深化（2026-10-15）

## 目的と参照実装

nanami-products の FULL に含まれる年運と同程度の計算根拠を、nanami-astro の鑑定者向け画面・AI入力・引き継ぎYAMLでも使えるようにする。

比較した実装は `../nanami-products/services/shichusuimei_calc.py` の `_annual_fortune` と `services/yaml_exporter.py`。Products のファイルは変更していない。astro に既存の月令・五行力量・格局候補・推奨五行候補・相互作用・神殺の計算を維持し、既存の3年分の流年を拡張した。

## 計算とデータ

- `normalized_data.timing` を正本とし、エンジン識別子を `shichu_timing_v2` に変更。
- 各流年に立春から翌立春までのタイムゾーン付き期間、参照日時、干支・通変星・十二運・蔵干と通変星・五行・日柱基準の空亡を追加。
- 各年の参照日時点の大運を選ぶ。未来の年に現在の大運を使い回さない。
- 大運切替／起運の目安が年内にある場合、`daiun_overlaps` に前後それぞれの大運と流年の作用を保持する。起運は既存の簡易換算のままで、切替日を精密計算したものではない。
- 命式×流年、大運×流年、命式＋大運＋流年の三合・方合、伏吟・反吟、三支が揃う三刑を区別する。三刑は成立に必要な三つの柱を保持し、大運が必要な成立を命式単独の成立と混同しない。
- `normalized_data.annual_fortune` と `features.annual_fortune` は同じ基準日データから生成する。Products と主要フィールド名を揃えているが、作用の各項目は astro の既存 `rule_id / pillars / members / provisional` 形式であり、Products への完全なスキーマ互換を意図しない。
- 年運の `year` は立春基準の干支年。`annual_fortune.year` は基準日の西暦年、`annual_fortune.effective_year` が干支年。例えば2026年1月は2025年の乙巳。
- 日付のみの基準日はその地域の00:00として計算する。立春当日は開始時刻に注意する。

## 表示と鑑定

- 結果画面の命式表示とテキスト出力に、大運と3年分の年運・計算根拠を表示。
- 占い師用分析では、既存の命式バランスに月令・力量・格局と反証・推奨五行候補／保留理由を追加。年別に根拠、現れ方の仮説、行動案、確認質問を表示。
- 初回の大運を「現在の大運」と説明していた処理を修正。現在大運がない旧データでは特定不能として扱う。
- AI用の共通指示 `prompts/shichu_full_depth.txt` を、四柱単独・西洋×四柱・三術統合に適用。通常本文は既存章に統合し、占い師メモは分野別の検討と確認質問まで扱う。
- 外部鑑定書のHTML生成経路と章JSON生成経路にも年運の使用指示を反映。
- 四柱単独の構造要約から命式・年運の詳細が落ちていた処理を修正。FULL引き継ぎYAMLと保存済み構造要約からも年運を利用できる。
- 出生時刻未入力をフォームが正午に正規化する場合も、元の `birth_time` の未入力情報を尊重。実際の正午生まれと区別し、時柱を含む作用を暫定にする。

合化や用神の確定判定、病気・事故・成功の断定は追加していない。生成済みの保存鑑定書は自動更新されない。旧ログに年運の基礎データがない場合は再解析が必要。

## 検証

対象は `tests/test_shichu_annual_reading.py` と既存の四柱推命・AIプロンプト・外部鑑定書・Vedic信頼性テスト。AI APIはスタブで入力内容を検証し、実際の有料生成は行っていない。DB接続先を `sqlite:///:memory:` に固定して実行。

```powershell
$env:DATABASE_URL='sqlite:///:memory:'
python -m pytest tests/test_shichu_annual_reading.py tests/test_shichu_timing.py tests/test_shichu_structure.py tests/test_shichu_interactions.py tests/test_ai_report_prompt_selection.py tests/test_external_report_prompt_safety.py tests/test_vedic_reliability.py -q
```

検証結果は69件PASS。別途、結果テンプレートのJavaScript構文と、実際の計算結果から3年分の表示項目が作られることをNode.jsで確認済み。変更したPythonの11ファイルの構文検査と `git diff --check` も通過。必要な既存依存SQLAlchemyは検証用の一時領域に配置し、リポジトリの依存定義は変更していない。成果物・一時ファイルはOneDrive外に置いた。

実装完了時点では本番デプロイ・DB更新・外部公開は実施していない。その後のユーザーのリリース指示に基づく反映結果は以下のとおり。

## 本番リリース（2026-10-15 21:42 JST 確認）

- Cloud Run: project `nanami-astro` / region `asia-northeast1` / service `nanami-astro`。
- 旧本番: `nanami-astro-00924-fup`。
- 新本番: `nanami-astro-shichu-1015-2134`。トラフィック100%を確認。
- 本番URL: https://nanami-astro-qel7k5utuq-an.a.run.app
- Cloud Build: `b2d116b9-d9bd-4f70-b4b6-bd91188f3bd4`、SUCCESS。
- リリースイメージ: `asia-northeast1-docker.pkg.dev/nanami-astro/cloud-run-source-deploy/nanami-astro@sha256:bc835b2887122c23057a9644294d12f9050a31757f0b0a83439973757ce076dc`。
- ビルド元: `C:\Users\haser\work\staging\nanami-shichu-release-20261015-2134`。`release_manifest.json` に14ファイルのSHA-256を記録。

稼働中の固定イメージ `sha256:b6115d58fb64f6b51ba15a8c549c7446e77e5f1eea7acede0ccbb2452080ff2e` をベースに今回の14ファイルだけを重ねた。既存の受注・メール・note等の未コミット差分と依存再インストールは含めない。旧本番ソースとGit HEADで、置換する既存ファイルの内容が一致していることを事前確認した。

新イメージ内の検証では、計算・FULL引き継ぎ・占い師要約・HTML出力・時刻不明判定、`/analyze` の四柱単独／西洋×四柱／三術統合、`/astrologer-result` と外部鑑定書プロンプトをすべて確認した。ここではSQLiteメモリDBとAPIキーなしの環境を使用。

トラフィック0%の候補URLで年運データと占い師画面を確認してから100%へ切り替えた。切り替え後の本番URLでも、トップ200、四柱計算API200、2026〜2028年の年運・根拠・質問を含む占い師画面200、出生地不足時の既存の日本語HTMLエラー400を確認した。計算APIと占い師画面はサーバー側約0.3秒／約0.03秒で応答した。出生時刻不明のケースも候補URLで確認済み。

### 検証中に確認した既存動作

`/analyze` は `yaml_only=true` / `generate_ai=false` を受け取っても、既存の `services/analyze_engine.py:run_single` がAI生成を呼ぶ。このため候補URLでの最初の2回は実際のAI生成が走り、四柱単独は約15秒、西洋×四柱は約65秒でサーバー側200となった。後者は検証クライアントの60秒タイムアウトを超えたため、以降のオンライン検証はAIを呼ばない `/api/calc/shichu` と占い師画面で実施した。この既存動作は今回のリリースに追加変更していない。案件への保存・納品・手動の本番DB操作は行っていない。

### 復旧

旧リビジョンと既存タグを維持している。必要時は次のトラフィック切替で旧版へ戻せる。

```powershell
gcloud run services update-traffic nanami-astro --project nanami-astro --region asia-northeast1 --to-revisions nanami-astro-00924-fup=100 --quiet
```

## 追加改善：年運10年と解析結果への常時表示

「解析画面で見当たらない」「10年分を表示したい」というフィードバックに対応。

- 流年の標準計算期間を3年から10年へ拡張。基準日の流年を含み、2026-10-15の解析なら2026〜2035年を返す。
- 前回は年運が「データ」タブの命式表の下にあったため、`templates/_shichu_annual_panel.html` を最初に開く「鑑定結果」タブへ追加。年の見出しを10件並べ、各年を開くと通変星・十二運・大運、蔵干・空亡・命式との作用と切替前後の根拠を確認できる。
- 年運欄はサーバー側でHTMLへ描画し、チャート用JavaScriptに依存しない。通常解析と、スタッフの保存済み結果画面が同じテンプレートを使うため、両方へ適用される。
- 入力画面に年運10年の案内を追加し、四柱推命の選択肢も「四柱・大運・年運10年」と明記。四柱を含まない結果にも、表示に必要な設定を案内する。
- 既存の保存結果を推測で10年へ水増しせず、保存データの実際の件数と再解析の案内を表示する。構造要約に四柱データがある場合も表示できる。
- YAML・JSON・計算API・占い師用分析・テキスト・外部出力も同じ10年分の計算を使用。AI用指示は計算済みの全年度を年別に扱うよう補足した。

関連テスト73件PASS。初期タブに10年度のHTMLがあること、JSなしの表示、短い旧ログ、四柱未選択時の案内、FULL引き継ぎと2035年までの出力を確認。ブラウザーでも10年の一覧と年別の開閉を確認した。

### 10年表示版の本番反映（2026-10-15 22:47 JST 確認）

- リビジョン：`nanami-astro-shichu10-1015-2242`、トラフィック100%。
- Cloud Build：`421c2f99-ec53-4b43-85e2-3b172f21fcb7`、SUCCESS。
- イメージ：`asia-northeast1-docker.pkg.dev/nanami-astro/cloud-run-source-deploy/nanami-astro@sha256:2314481f4bb68c83a3e22710f3f5d3be2319f61d03312694df366668647b6291`。
- ビルド元：`C:\Users\haser\work\staging\nanami-shichu10-release-20261015-2242`。直前の固定イメージに今回の実行用5ファイルだけを重ね、マニフェストにSHA-256を記録。
- コンテナ内で四柱単独・西洋×四柱・三術統合の実際の `/analyze` 応答を検証。全経路で最初の結果タブに10年分のHTMLがあることを確認。APIキーなし・SQLiteメモリDBで実施。
- トラフィック0%の候補で時刻既知／不明、10年分の年運と占い師画面、入力案内を確認後に本番へ切り替えた。
- 普段利用する `https://nanami-astro-992374440214.asia-northeast1.run.app/` で、入力案内・年運10年・2035年の大運・YAML・占い師画面の全年度を確認。オンライン検証はAIを呼ばない計算APIを使用し、案件への保存は行っていない。
- 直前の3年表示版 `nanami-astro-shichu-1015-2134` はタグ付きで維持。必要時はこのリビジョンへ100%切り戻せる。
