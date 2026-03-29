
# --- Example modification for src/services/handoff_log.py ---

def build_handoff(
    astro_result,
    reports,
    inputs,
    payload,
    unknowns,
    mode="mini",
    prev=None,
    topic="",
    message="",
    observations_text="",
):
    handoff = {
        "inputs": inputs,
        "payload": payload,
        "topic": topic,
        "message": message,
        "observations_text": observations_text,
        "reports": reports,
        "unknowns": unknowns,
    }

    return handoff
