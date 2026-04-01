
# --- Example modification for src/services/ai_report.py ---

def build_prompt_context(meta: dict):
    meta2 = meta or {}

    topic = (meta2.get("topic", "") or "").strip()
    message = (meta2.get("message", "") or "").strip()
    observations_text = (meta2.get("observations_text", "") or "").strip()

    prompt_vars = {
        "topic": topic,
        "message": message,
        "observations_text": observations_text,
    }

    return prompt_vars
