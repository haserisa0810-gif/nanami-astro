from services.ai_report import _read_prompt_file, _render_prompt, _select_prompt_files


def _select(system: str, *, use_claude: bool = True, reading_style: str = "general") -> str:
    files = _select_prompt_files(
        system, "single_web", "web", "overall",
        use_claude=use_claude, reading_style=reading_style,
    )
    return files["single_web"]


def test_shichusuimei_claude_uses_dedicated_prompt_not_western_one():
    # バグ再発防止: 四柱単独選択で西洋前提の single_web_claude.txt に落ちない
    assert _select("shichusuimei") == "single_web_claude_shichu.txt"
    # reading_style を変えても四柱専用プロンプトを維持する
    assert _select("shichusuimei", reading_style="timing") == "single_web_claude_shichu.txt"


def test_existing_prompt_selection_is_unchanged():
    assert _select("integrated_w_shichu") == "single_web_claude_w_shichu.txt"
    assert _select("integrated_w_shichu", reading_style="love") == "single_web_claude_w_shichu_love.txt"
    assert _select("western") == "single_web_claude.txt"
    assert _select("western", reading_style="work") == "single_web_claude_work.txt"


def test_shichu_prompt_renders_and_contains_western_ban():
    template = _read_prompt_file("single_web_claude_shichu.txt")

    # 禁止指示が含まれる
    for banned_keyword in ("トランジット", "ASC", "ハウス", "アスペクト", "ナクシャトラ"):
        assert banned_keyword in template
    # 時期の章は大運・流年ベース
    assert "normalized_data.timing" in template
    assert "トランジットを使い" not in template

    # 深化データの活用指示と用語の厳密ルール
    assert "補助情報として関連がある場合だけ" in template
    assert "hold/withheld は保留" in template
    assert "その type がある場合のみ使う" in template
    assert "関連が薄い章は統合・省略" in template

    # プレースホルダが ctx で全て埋まる（KeyError にならず {} が残らない）
    rendered = _render_prompt(template, {
        "common_rules": "", "astro_data": {"summary": {}}, "structure_summary": "{}",
        "astrology_system": "shichusuimei", "theme": "overall", "user_message": "",
        "era_title": "いまの転換期", "age_years": 40, "today": "2026-07-14",
        "detail_level": "standard",
    })
    assert "{astro_data}" not in rendered
    assert "{era_title}" not in rendered


def test_guard_shichu_exists_and_bans_fabrication():
    guard = _read_prompt_file("guard_shichu.txt")
    for keyword in ("星座", "トランジット", "禁止"):
        assert keyword in guard
