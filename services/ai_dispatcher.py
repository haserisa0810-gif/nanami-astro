# services/ai_dispatcher.py

from __future__ import annotations

CLAUDE_HAIKU_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_SONNET_MODEL = "claude-sonnet-4-6"


def resolve_ai_model(model_key: str):
    """
    UIで選択されたモデルキーを(provider, model_name)に変換する。
    Anthropic APIキーはHaiku/Sonnet共通で使える。モデル名だけ切り替える。
    """
    model_map = {
        "claude_haiku": ("claude", CLAUDE_HAIKU_MODEL),
        "claude_sonnet": ("claude", CLAUDE_SONNET_MODEL),
        "gemini_flash": ("gemini", "gemini-2.0-flash"),
        "gemini_flash_light": ("gemini", "gemini-2.0-flash-lite"),
        "gemini_pro": ("gemini", "gemini-2.5-pro"),
        # 新UI互換
        "haiku": ("claude", CLAUDE_HAIKU_MODEL),
        "sonnet": ("claude", CLAUDE_SONNET_MODEL),
        "flash-lite": ("gemini", "gemini-2.0-flash-lite"),
        "flash": ("gemini", "gemini-2.0-flash"),
        "pro": ("gemini", "gemini-2.5-pro"),
    }

    if not model_key:
        return ("claude", CLAUDE_HAIKU_MODEL)

    return model_map.get(model_key, ("claude", CLAUDE_HAIKU_MODEL))


def is_high_quality(model_key: str) -> bool:
    """高品質モデルかどうか判定。"""
    return model_key in [
        "claude_sonnet",
        "gemini_pro",
        "sonnet",
        "pro",
    ]


def is_fast_model(model_key: str) -> bool:
    """軽量モデル判定。"""
    return model_key in [
        "claude_haiku",
        "gemini_flash",
        "gemini_flash_light",
        "haiku",
        "flash",
        "flash-lite",
    ]
