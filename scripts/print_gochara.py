from __future__ import annotations

"""note月次記事用のゴーチャラ（Lahiri sidereal）だけを手早くダンプするCLI。

`/products/note-articles` の記事生成はAIで全文を作る重い経路なので、
「全体の星の流れ」に差し込むゴーチャラのブロックだけが欲しいときはこちらを使う。
計算ロジックは services.note_article_service / services.vedic_calc の既存関数をそのまま再利用する。

使い方:
    python scripts/print_gochara.py            # 今月（default_target_month）
    python scripts/print_gochara.py 2026-08    # 月を指定（YYYY-MM）
    python scripts/print_gochara.py 2026-08 --json   # 生JSONも一緒に出す
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.note_article_service import (  # noqa: E402
    default_target_month,
    extract_note_vedic_context,
    validate_target_month,
    _vedic_gochara_for_prompt,
)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    want_json = "--json" in args
    args = [a for a in args if a != "--json"]

    target_month = args[0] if args else default_target_month()
    validate_target_month(target_month)  # 不正な月は早期に弾く

    ctx = extract_note_vedic_context(target_month)
    prompt_block = _vedic_gochara_for_prompt(ctx)

    print(f"# ゴーチャラ {target_month}（Lahiri sidereal / 月初・中旬・月末）")
    print()
    print(prompt_block.strip() if prompt_block else "(ゴーチャラを取得できませんでした)")

    if want_json:
        print()
        print("--- raw JSON ---")
        print(json.dumps(ctx, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
