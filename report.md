# nanami-astro 調査レポート

## 総評

**全体としての安定性:** 中程度。占術計算コア（`western_calc` / `vedic_calc` / `shichusuimei_calc` / `transit_calc`）は timezone を JST→UTC で一貫処理しており、計算部分の作りは堅実です。一方で、**運用面・セキュリティ面に本番事故につながりうる弱点が複数**あります。特に「公開URLの期限管理の不統一」「機密情報のログ出力」「`deploy.env.yaml` に平文シークレットが入っている状態」が目立ちます。

**特に危ない領域:**
1. **シークレット管理** — `deploy.env.yaml` に DB パスワード・API キー・管理者初期パスワード・SMTP パスワードが平文。`.gitignore` 対象ではあるが、ファイルがローカルに実在し流出リスクが高い。
2. **公開URLの期限・秘匿性の不統一** — 外部受注の共有URLは期限＋noindexありだが、`/t/{token}`（トランジット商品）と `/report/{order_code}`（受注結果）は期限もnoindexもなく、個人情報を含むページが半永久的に閲覧可能。
3. **個人情報のログ出力** — LINE Webhook がリクエストボディ全文と生年月日等を `print` している。

**すぐ修正すべきか様子見か:** Critical項目（後述）は早急な対応推奨。ただし今回は調査のみのため実装は未着手です。

---

## 重大度別一覧

### Critical
- C-1: 公開結果ページ `/result/{order_code}` `/report/{order_code}` に認可・期限・noindexがなく、`order_code` 推測で他人の鑑定書が見える可能性
- C-2: `/t/{token}` トランジット商品の公開HTMLに期限切れ判定もnoindexも無い（個人情報が半永久公開）
- C-3: `deploy.env.yaml` に本番相当のシークレットが平文で存在
- C-4: LINE Webhook が個人情報を含むリクエストボディを標準出力へログ

### High
- H-1: `ExternalOrder.expires_at` 未設定時は共有URLが無期限（デフォルト発行フローで期限がセットされない）
- H-2: `LINE_CHANNEL_SECRET` 未設定時に署名検証がスキップされ、Webhookが無検証で受理される
- H-3: `analyze_engine.py` がリポジトリ内に3系統（ルート `analyze_engine.py`＝LEGACY、`services/analyze_engine.py`＝ACTIVE、`services/services_analyze_engine.py`＝重複候補）存在し、取り違えリスク
- H-4: 管理・スタッフの注文一覧がページング無しで全件ロード（件数増でメモリ/レスポンス劣化）
- H-5: `_ensure_stores_payment_table` がリクエスト毎に `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX` のDDLを実行

### Medium
- M-1: `free_wait.html` のポーリングが無制限（生成が `running` で固まると永遠に待機）
- M-2: 出生地ジオコーディングが同期 `requests`（Nominatim, timeout=5s）で、注文送信フロー内に組み込まれ、外部依存で詰まる
- M-3: 座標が都道府県代表点にフォールバックしても、YAML/結果に「概算である」明示が弱い（ハウス・ASCが実際とズレるが利用者は気づけない）
- M-4: `except Exception: pass` 系が多数（約29箇所）でエラーが握り潰され、原因追跡が困難
- M-5: Claude/Gemini呼び出しのトークン上限超過・truncation の検知が無い（途中で切れたHTML/JSONがそのまま保存されうる）
- M-6: `astrologer-result` などフォーム受けの JSON をそのまま描画する経路で XSS 面の確認が必要
- M-7: STORES注文照合が「注文番号のみ一致・メール不一致」でも `needs_review` 止まりで、番号を知る第三者が受付を上書きしうる余地

### Low
- L-1: 多数の管理系テンプレートに `viewport` メタ無し（スマホで崩れる）
- L-2: プロンプト・サービスに「コピー」重複ファイルや `LEGACY`/`UNUSED_CANDIDATE` マーカー多数（保守性低下）
- L-3: 英語展開時に日本語決め打ちの箇所が多い（都道府県テーブル、星座和名変換、プロンプト全文、`gender` コード '1'〜'4'）
- L-4: `app.db`（SQLite, 372KB）がリポジトリに含まれている
- L-5: `PUBLIC_ORDER_BASE_URL` の値が `https://pay.nanami-astro.comnanami-asutro-daily-card` と連結崩れの疑い（要確認）

---

## 各指摘の詳細

### C-1: 公開結果ページに認可・期限・noindexが無い
- **重大度:** Critical
- **該当ファイル:** `routes_public_orders.py`
- **該当箇所:** `order_result`（1270行〜）、`order_report`（1298行〜）、`free_reading_result`（1234行〜）
- **内容:** `/result/{order_code}` `/report/{order_code}` は `order_code` だけで氏名・生年月日・出生地・相談内容・鑑定本文を返す。認証なし、期限なし、`X-Robots-Tag` なし。
- **なぜ問題か:** `order_code` は `A + uuid4().hex[:10].upper()`（`order_service.py:13`）で推測困難だが、URLはメール・LINE・ブラウザ履歴・リファラで漏れうる。漏れた場合の失効手段が無く、検索エンジンにインデックスされる可能性もある。外部受注共有(`/report/share`)は期限＋noindexを実装済みなのに、この経路だけ無防備という不整合。
- **確認方法:** ログアウト状態で `/result/<既知のorder_code>` にアクセス→本文が表示される。レスポンスヘッダに `X-Robots-Tag` が無いことを確認。
- **修正方向性:** 署名付きトークン（外部受注と同じ `URLSafeSerializer`/HMAC 方式）へ統一、`X-Robots-Tag: noindex`＋`Cache-Control: private, no-store` 付与、任意で有効期限。
- **修正優先度:** 高
- **様子見可否:** 不可（個人情報＋納品物）。

### C-2: `/t/{token}` に期限・noindexが無い
- **重大度:** Critical
- **該当ファイル:** `routes_products.py:513`（`public_transit_html`）、`models.py`（`TransitHubRequest`）
- **該当箇所:** `TransitHubRequest` に `expires_at` カラムが存在しない。`public_transit_html` は token 一致で `generated_html` をそのまま返すのみ。
- **なぜ問題か:** トランジット鑑定書は氏名・生年月日等を含む。無期限・noindexなしで、外部受注側(`external_report_share`)と保護レベルが揃っていない。
- **確認方法:** `/t/<token>` へアクセスし、レスポンスにnoindexヘッダが無いこと・期限判定コードが無いことを確認。
- **修正方向性:** 外部受注と同じ noindex/no-store ヘッダ付与、`expires_at` 追加と失効判定。
- **修正優先度:** 高
- **様子見可否:** 不可。

### C-3: `deploy.env.yaml` の平文シークレット
- **重大度:** Critical
- **該当ファイル:** `deploy.env.yaml`
- **該当箇所:** `DATABASE_URL`（Neon DBの資格情報）、`ADMIN_SESSION_SECRET`/`READER_SESSION_SECRET`、`DEFAULT_ADMIN_PASSWORD`、`LINE_CHANNEL_ACCESS_TOKEN`/`SECRET`、`GEMINI_API_KEY`、`GOOGLE_MAPS_API_KEY`、`SMTP_PASSWORD`、`STORES_MAIL_PASSWORD` 等。
- **なぜ問題か:** `.gitignore`/`.dockerignore` 対象で Git には未追跡だが、実ファイルが平文で存在。誤コミット・バックアップ・端末流出時に全系統が同時に漏れる。特に `DATABASE_URL` に接続文字列がフル、`DEFAULT_ADMIN_PASSWORD` が既知になると管理画面奪取につながる。
- **確認方法:** `git ls-files | grep deploy.env.yaml`（追跡されていないことは確認済み）。ファイル自体は平文で存在。
- **修正方向性:** Secret Manager 等へ移行、鍵のローテーション、`DEFAULT_ADMIN_PASSWORD` 初回ログイン強制変更（`is_temp_password` 機構は既にあるので初期パスは一時扱いで問題ないが、値の秘匿は必要）。
- **修正優先度:** 高
- **様子見可否:** 不可。**要確認:** 現在 Cloud Run へどの経路で環境変数を注入しているか（このファイル直投入か、Secret Manager併用か）。

### C-4: LINE Webhook の個人情報ログ
- **重大度:** Critical（プライバシー）
- **該当ファイル:** `line_webhook.py:214-229`
- **該当箇所:** `print("LINE webhook body:", body[:500])` / `print("LINE payload parsed:", payload)` / `print("LINE signature header:", signature)`
- **なぜ問題か:** ユーザーの発話（氏名・生年月日を含む注文入力）や userId が Cloud Run ログに平文で残り続ける。同種の `print` は `free_reading_service.py`・`analyze_engine.py`・`report_generation_service` 等にも散在。
- **確認方法:** LINEから注文フローを進め、Cloud Run ログに本文が出ることを確認。
- **修正方向性:** 本文ログを削除またはマスキング、構造化ロガーへ移行しレベル制御。署名ヘッダもログ不要。
- **修正優先度:** 高
- **様子見可否:** 不可。

### H-1: 外部受注共有URLの無期限化
- **重大度:** High
- **該当ファイル:** `routes_external_orders.py:871`（`issue-url`）
- **該当箇所:** `issue_url`/`reissue_url` は `expires_at` をセットしない。`expires_at` は `update` フォームで手入力した時のみ入る。共有時の期限チェックは `external_report_share`（1020行）に有るが、`expires_at` が None だと素通り。
- **なぜ問題か:** スタッフが期限を明示入力しない限り、共有URLは永久有効。
- **修正方向性:** URL発行時にデフォルト期限（例：発行から90日）を自動設定。
- **修正優先度:** 中〜高
- **様子見可否:** 運用ルールで期限を必ず入れているなら様子見可。**要確認:** 現状の運用ルール。

### H-2: LINE署名検証のフェイルオープン
- **重大度:** High
- **該当ファイル:** `line_webhook.py:219`
- **該当箇所:** `if secret and not _verify_line_signature(...)`
- **内容:** `LINE_CHANNEL_SECRET` が空だと検証を丸ごとスキップして全リクエストを受理。
- **なぜ問題か:** 設定漏れ・環境変数事故で偽装Webhookを受け付け、なりすまし注文やセッション操作の入口になる。
- **修正方向性:** secret 未設定時は 500 で明示的に停止（フェイルクローズ）。
- **修正優先度:** 中
- **様子見可否:** 本番で secret が確実に入っているなら実害は低いが、設計としては要修正。

### H-3: `analyze_engine` 三重化
- **重大度:** High（保守性・事故誘発）
- **該当ファイル:** `analyze_engine.py`（`# LEGACY_OLD_LOCATION`）、`services/analyze_engine.py`（`# ACTIVE_MAIN_ENGINE`）、`services/services_analyze_engine.py`（`# LEGACY_DUPLICATE_CANDIDATE`）
- **内容:** 実importは全て `services.analyze_engine`。ルート直下と `services_analyze_engine` は未使用の重複。
- **なぜ問題か:** 将来「どれを直せばいいか」を誤り、修正が反映されない/退行する典型パターン。
- **修正方向性:** 未使用2ファイルを削除（import参照が無いことを確認済み）。
- **修正優先度:** 中
- **様子見可否:** 可だが早めの整理推奨。

### H-4: 注文一覧のページング無し
- **重大度:** High（将来のパフォーマンス）
- **該当ファイル:** `routes_admin.py:241`、`routes_staff.py:533`
- **内容:** フィルタ後 `.all()` で全件をテンプレートに渡す。`admin_customers`（471行）も全 Customer をロード。
- **なぜ問題か:** 注文が数千件規模になると1リクエストで全件 SELECT＋レンダリングし、遅延・メモリ増。
- **修正方向性:** LIMIT/OFFSET かカーソルページング導入。
- **修正優先度:** 中（件数次第）
- **様子見可否:** 現状件数が小さいなら可。

### H-5: リクエスト毎DDL
- **重大度:** High（設計）
- **該当ファイル:** `routes_public_orders.py:97,487,606`
- **内容:** STORES照合のたび `_ensure_stores_payment_table` が `CREATE TABLE/INDEX IF NOT EXISTS` を実行し `commit`。
- **なぜ問題か:** 本来マイグレーションで一度作るべきものをホットパスで毎回実行。無駄なDBラウンドトリップ、権限・ロック観点でも望ましくない。しかも `stores_payments` は SQLAlchemy モデル化されておらず raw SQL 管理。
- **修正方向性:** 起動時 `init_db` 側へ移動、モデル化を検討。
- **修正優先度:** 中
- **様子見可否:** 可。

### M-1: 無料鑑定 待機ポーリング無制限
- **該当ファイル:** `templates/free_wait.html:9-24`
- **内容:** `status !== completed/failed` の間 3秒ごとに永久ポーリング。`process_free_reading` が `running` のままハングした場合、ユーザーは永遠に待つ。
- **修正方向性:** 最大試行回数/タイムアウト表示、`running` の stale 検知（外部受注の `_mark_stale_generation_if_needed` に相当する仕組みが無料・スタッフ側にもあると良い）。

### M-2: ジオコーディングが同期・外部依存
- **該当ファイル:** `services/location.py:165`（`geocode_place`）、`routes_public_orders.py:910` などの注文フロー内で `resolve_birth_location` 呼び出し。
- **内容:** Nominatim へ同期 `requests`（timeout=5s）。都道府県のみなら代表座標で済むが、市区町村入力時は毎回外部APIを叩く。
- **なぜ問題か:** Nominatim の遅延・レート制限（`User-Agent: nanami-astro/1.0`、利用規約上1req/s）で注文送信が詰まる/失敗フォールバック。失敗時は代表座標に落ちるが利用者は精度低下に気づかない。
- **修正方向性:** 非同期化・キャッシュ・市区町村CSVによるローカル解決、フォールバック時の明示。

### M-3: 座標フォールバックの不透明さ
- **該当ファイル:** `services/location.py`（`prefecture_center` / `prefecture_center_fallback`）、`shared.py:_calc_payload_from_inputs`
- **内容:** 出生時刻未入力時は 12:00 固定（`unknowns` に記録）だが、座標が都道府県庁所在地に丸められた場合、ASC/MC/ハウスは実出生地とズレる。結果画面の `unknowns` 表示は時刻には言及するが、座標近似の注意喚起は弱い。
- **なぜ問題か:** 鑑定結果の信頼性に直結。特にハウス系の解釈が「県代表点」ベースだと知らずに納品される。

### M-4: 例外の握り潰し多数
- **該当ファイル:** 全体（`except Exception: pass` 約29件）。例：`ai_report.py`（メタ抽出）、`handoff_log.py`、`western_calc.py:257`（vertex取得）等。
- **なぜ問題か:** 計算欠損やパース失敗が黙って空データになり、YAML/本文の欠落として表面化しても原因が追えない。
- **修正方向性:** 少なくとも warning ログを残す。

### M-5: LLM出力の途中切れ検知なし
- **該当ファイル:** `services/report_generation_service.py:650`（`generate_text_with_claude`）、`external_report_template_renderer.py:659`（`parse_chapter_json`）
- **内容:** ストリームは timeout 監視するが、`max_tokens` 到達による truncation を検知していない。`parse_chapter_json` は壊れたJSONを正規表現で救おうとし、失敗章は「本文生成に失敗しました。再生成してください。」で埋める。
- **なぜ問題か:** プレミアム13章＋根拠欄は 32k tokens に収まらない可能性があり、末尾章が欠落したHTMLがそのまま保存・共有されうる。`stop_reason` を見ていないため成功扱いになる。
- **修正方向性:** `stop_reason == "max_tokens"` を検知して失敗扱い/再試行。

### M-6: フォーム経由JSONの描画
- **該当ファイル:** `routes_public_pages.py:101`（`astrologer_result`）、`templates/astrologer_result.html`（未精読）
- **内容:** POSTされた `result_json`/`reader_text` をパースしテンプレートへ。Jinja2自動エスケープ前提だが、`result.html`/レポートHTMLは `|safe` で本文を描画する箇所があるため、AI生成HTMLやユーザー入力がHTMLとして注入される経路が無いか要確認。
- **修正優先度:** 中（**要確認**）。

### M-7: STORES照合の上書き余地
- **該当ファイル:** `routes_public_orders.py:872-946`、`_verify_stores_payment`（613行）
- **内容:** 既存注文の判定キーは `external_order_ref + user_contact`。メール不一致は `needs_review` で弾くが、`verified_without_email`（買主メールがメールに含まれない通知の場合）を許容。注文番号＋任意の連絡先で受付が成立し、既存注文を上書き更新するフローがある（immutable ステータス以外）。
- **なぜ問題か:** STORES注文番号を知る第三者が別人の生年月日で受付内容を差し替える余地。
- **修正優先度:** 中。**要確認:** STORES通知メールに購入者メールが必ず含まれるか。

### L項目（要点のみ）
- **L-1:** `admin_*`, `external_*`, `reader_*`, `products_*` 等の多数テンプレートに `viewport` 無し → スマホ表示崩れ（`templates/*.html`）。
- **L-2:** `prompts/` に `guard_integrated (コピー).txt` 等の重複、`services/*.py` に `LEGACY`/`UNUSED_CANDIDATE` マーカー多数 → 削除候補整理を。
- **L-3:** 英語展開の障害：`services/location.py` の都道府県ハードコード、星座和名変換（`external_report_template_renderer`）、プロンプト全文が日本語、`gender` が `'1'〜'4'`/`male|female|...` と表現ゆれ。
- **L-4:** `app.db`（SQLite）がツリーに存在。テスト/開発用と思われるが本番はPostgres。混乱・情報残留の元。**要確認:** 中身に実顧客データが無いか。
- **L-5:** `deploy.env.yaml` の `PUBLIC_ORDER_BASE_URL: https://pay.nanami-astro.comnanami-asutro-daily-card` が連結崩れに見える。**要確認**。

---

## 仕様確認が必要なもの

1. Cloud Run へのシークレット注入経路（`deploy.env.yaml` 直投入か Secret Manager か）。
2. `ExternalOrder.expires_at` を運用でどこまで必須にしているか（H-1）。
3. `/result` `/report` `/t` の公開URLを「誰でもアクセス可」で設計しているのか、それとも本人限定の想定か（C-1/C-2）。
4. STORES通知メールに購入者メールアドレスが必ず含まれるか（M-7・`verified_without_email` の許容範囲）。
5. `app.db` に実顧客データが含まれていないか（L-4）。
6. プレミアム13章が Claude Sonnet の 32k output に確実に収まる実績があるか（M-5）。
7. `astrologer_result.html` 等で AI生成HTML/ユーザー入力を `|safe` 描画している箇所のXSS可否（M-6）。

---

## 今回は修正しないが、将来見直した方がよい設計

- **スキーマ管理の一本化:** `bootstrap_platform.py` の `ALTER TABLE ADD COLUMN` 逐次実行＋リクエスト毎DDL（`stores_payments`）を、Alembic等のマイグレーションへ移行。`stores_payments` のモデル化。
- **公開URLの共通化:** 受注・外部受注・トランジット・スタッフレポートで token 方式・期限・noindex がバラバラ。単一の「公開共有トークン」基盤（HMAC署名＋expires＋noindex＋失効）へ統合。
- **エンジンモジュールの整理:** `analyze_engine` 三重化と `LEGACY`/`UNUSED_CANDIDATE` の掃除。
- **ロギング基盤:** 全面 `print` から構造化ロガー＋PIIマスキングへ。
- **多言語化前提の分離:** 都道府県・星座名・プロンプト・性別コードのi18n化。
- **LLM生成の堅牢化:** `stop_reason` 監視、章単位リトライ、生成物のバリデーション（必須章・必須根拠欄の存在確認）を保存前に実施。
- **注文一覧のページング/インデックス見直し。**

---

## まず着手するなら（提案・今回は実装しない）

1. **C-4 / C-3:** ログのPII除去とシークレットの Secret Manager 移行＋鍵ローテーション（漏洩の即時リスク低減。コード変更小）。
2. **C-1 / C-2:** 公開結果ページに `noindex` + `no-store` ヘッダを付与（1行レベルで着手可能）、続けてトークン化・期限化。
3. **H-1:** URL発行時のデフォルト期限自動設定。
4. **H-2:** LINE署名検証のフェイルクローズ化。
5. **M-5:** LLM出力の `stop_reason` 検知（プレミアム納品事故防止）。
6. **H-3 / L-2:** 未使用エンジン・重複ファイルの削除で以後の修正取り違えを予防。
