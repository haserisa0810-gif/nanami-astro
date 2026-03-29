from __future__ import annotations

from pathlib import Path

import swisseph as swe


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ephe_dir() -> Path:
    return _project_root() / "ephe"


def configure_ephemeris() -> int:
    ephe_dir = _ephe_dir()
    if ephe_dir.exists() and any(ephe_dir.glob("*.se1")):
        swe.set_ephe_path(str(ephe_dir))
        return swe.FLG_SWIEPH
    return swe.FLG_MOSEPH


def ephemeris_debug_info() -> dict:
    ephe_dir = _ephe_dir()
    files = sorted(p.name for p in ephe_dir.glob("*")) if ephe_dir.exists() else []
    return {
        "ephe_dir": str(ephe_dir),
        "ephe_dir_exists": ephe_dir.exists(),
        "ephe_files": files,
        "has_se1": any(name.endswith(".se1") for name in files),
    }
