# Draft/Report foundation patch

このパッチで入れたもの:

- `intake_drafts` モデル追加
- `reports` モデル追加
- `orders.primary_report_id` / `orders.input_origin` 追加
- `bootstrap_platform.py` に新テーブル/新カラム作成と簡易バックフィル追加
- `services/draft_service.py` 追加
- `services/report_service.py` 追加
- `services/cleanup_service.py` 追加
- `services/order_service.py` に `create_order_from_draft` 追加
- `routes_public_orders.py` で注文作成時に draft も作成して order に昇格
- `services/analyze_save_service.py` で保存時に reports も更新
- `routes_admin.py` に cleanup 実行用 endpoint 追加
- `templates/staff_order_detail.html` に主レポート情報を追加

まだ段階的対応のため、現在のフォーム/注文フロー互換性を優先して以下は未完です。

- フォーム入力保存先を `orders` から `intake_drafts` に完全移行
- `/analyze` を `draft_id` ベースで直接動かす導線
- `YAMLのみ作成` UI の完全実装
- 章単位/4ブロック単位の report 生成 UI

まずは土台を入れて、既存フローを壊さず `draft/report` を蓄積できる状態に寄せています。
