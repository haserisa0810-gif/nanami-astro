# services/ai_dispatcher.py

from __future__ import annotations


def resolve_ai_model(model_key: str):
    """
    UIで選択されたモデルキーを
    (provider, model_name) に変換する

    例:
    claude_haiku -> ("claude", "claude-3-haiku")
    """
    model_map = {
        "claude_haiku": ("claude", "claude-3-haiku"),
        "claude_sonnet": ("claude", "claude-3-5-sonnet"),
        "gemini_flash": ("gemini", "gemini-2.0-flash"),
        "gemini_flash_light": ("gemini", "gemini-2.0-flash-lite"),
        "gemini_pro": ("gemini", "gemini-2.5-pro"),
        # 新UI互換
        "haiku": ("claude", "claude-3-haiku"),
        "sonnet": ("claude", "claude-3-5-sonnet"),
        "flash-lite": ("gemini", "gemini-2.0-flash-lite"),
        "flash": ("gemini", "gemini-2.0-flash"),
        "pro": ("gemini", "gemini-2.5-pro"),
    }

    if not model_key:
        return ("claude", "claude-3-haiku")

    return model_map.get(model_key, ("claude", "claude-3-haiku"))



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
