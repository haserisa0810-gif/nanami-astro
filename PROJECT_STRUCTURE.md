# nanami-astro 構成

- app.py: 起動エントリポイント
- routes.py: Web ルーティング
- templates/: 画面テンプレート
- static/: 静的ファイル置き場
- services/: 占術ロジック
- prompts/: AIレポート用テンプレート
- ephe/: Swiss Ephemeris データ

注: 今回の再構成は保守性を上げる目的が中心で、
軽量化の本命は Google Maps の遅延読み込みとモバイル演出の削減です。
