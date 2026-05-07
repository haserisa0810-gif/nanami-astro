from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover
    Anthropic = None  # type: ignore

from services.transit_calc import calc_global_transit_snapshot


DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_OUTPUT_DIR = Path("outputs/social_scenarios")
DEFAULT_VOICEVOX_URL = "http://127.0.0.1:50021"
DEFAULT_VOICEVOX_SPEAKER = 3
DEFAULT_META_GRAPH_VERSION = "v25.0"
DEFAULT_VIDEO_DURATION_SECONDS = 60
SOCIAL_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
SOCIAL_AUDIO_SUFFIXES = {".wav", ".mp3", ".m4a", ".aac"}

PLANET_JA = {
    "Sun": "太陽",
    "Moon": "月",
    "Mercury": "水星",
    "Venus": "金星",
    "Mars": "火星",
    "Jupiter": "木星",
    "Saturn": "土星",
    "Uranus": "天王星",
    "Neptune": "海王星",
    "Pluto": "冥王星",
}

ASPECT_JA = {
    "conjunction": "コンジャンクション",
    "opposition": "オポジション",
    "square": "スクエア",
    "trine": "トライン",
    "sextile": "セクスタイル",
}

ASPECT_KEYWORDS = {
    ("Mercury", "Pluto", "square"): "深掘り・気づき・本質",
    ("Mars", "Jupiter", "square"): "勢いすぎ注意・熱量の調整",
    ("Moon", "Neptune", "square"): "感受性・揺らぎ・整える",
    ("Sun", "Jupiter", "conjunction"): "広がり・自信・やりすぎ注意",
    ("Venus", "Uranus", "square"): "関係性の変化・距離感の更新",
    ("Saturn", "Neptune", "trine"): "理想を形にする・静かな調整",
}

SYSTEM_PROMPT = """あなたは星月七海（ほしつきななみ）という占星術師のアシスタントです。
毎日のアスペクト情報をもとに、TikTok・Instagram向けの
60秒ナレーションシナリオを作成します。

トーンの指定：
- 落ち着いていて、やわらかい語り口
- 押しつけがましくない、寄り添う表現
- 専門用語は最小限。感情・行動に結びつけた言葉を優先
- 絵文字は使わない（音声読み上げ用のため）

出力は必ず以下のラベルつきで返す：
【オープニング】
【アスペクト解説】
【今日のテーマ】
【アドバイス】
【締め】
"""


SAMPLE_ASPECTS = [
    {
        "planet1": "水星",
        "planet2": "冥王星",
        "aspect": "スクエア",
        "orb": 0.16,
        "keyword": "深掘り・気づき・本質",
    },
    {
        "planet1": "火星",
        "planet2": "木星",
        "aspect": "スクエア",
        "orb": 0.56,
        "keyword": "勢いすぎ注意・熱量の調整",
    },
    {
        "planet1": "月",
        "planet2": "海王星",
        "aspect": "スクエア",
        "orb": 0.99,
        "keyword": "感受性・揺らぎ・整える",
    },
]


@dataclass(frozen=True)
class ScenarioParts:
    date: str
    opening: str
    aspects: str
    theme: str
    advice: str
    closing: str

    @property
    def full_script(self) -> str:
        return "\n".join(
            [
                "【オープニング】",
                self.opening,
                "",
                "【アスペクト解説】",
                self.aspects,
                "",
                "【今日のテーマ】",
                self.theme,
                "",
                "【アドバイス】",
                self.advice,
                "",
                "【締め】",
                self.closing,
            ]
        ).strip()

    def as_dict(self) -> dict[str, str]:
        return {
            "date": self.date,
            "opening": self.opening,
            "aspects": self.aspects,
            "theme": self.theme,
            "advice": self.advice,
            "closing": self.closing,
            "full_script": self.full_script,
        }


def normalize_aspects(aspects: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in aspects or []:
        if not isinstance(item, dict):
            continue
        planet1 = str(item.get("planet1") or item.get("planet_a") or item.get("transit_planet") or "").strip()
        planet2 = str(item.get("planet2") or item.get("planet_b") or item.get("natal_planet") or "").strip()
        aspect = str(item.get("aspect") or "").strip()
        try:
            orb = float(item.get("orb", 99))
        except Exception:
            orb = 99.0
        if not planet1 or not planet2 or not aspect:
            continue
        rows.append(
            {
                "planet1": PLANET_JA.get(planet1, planet1),
                "planet2": PLANET_JA.get(planet2, planet2),
                "aspect": ASPECT_JA.get(aspect, aspect),
                "orb": round(orb, 2),
                "keyword": item.get("keyword") or _keyword_for(planet1, planet2, aspect),
            }
        )
    rows.sort(key=lambda x: float(x.get("orb", 99)))
    return rows[:limit]


def aspects_from_global_transit(target_date: date | None = None, *, limit: int = 3) -> list[dict[str, Any]]:
    target_dt = datetime.combine(target_date or date.today(), datetime.min.time()) if target_date else None
    snapshot = calc_global_transit_snapshot(target_date=target_dt)
    return normalize_aspects(snapshot.get("aspects", []), limit=limit)


def generate_scenario(aspects: list[dict[str, Any]]) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is required")
    if Anthropic is None:
        raise RuntimeError("anthropic SDK is not installed")

    normalized = normalize_aspects(aspects)
    if not normalized:
        normalized = SAMPLE_ASPECTS
    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model=os.getenv("SOCIAL_SCENARIO_CLAUDE_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        max_tokens=900,
        temperature=0.75,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_prompt(normalized)}],
    )
    return _extract_text(response)


def generate_mock_scenario(aspects: list[dict[str, Any]]) -> str:
    normalized = normalize_aspects(aspects) or SAMPLE_ASPECTS
    top = normalized[0]
    second = normalized[1] if len(normalized) > 1 else top
    third = normalized[2] if len(normalized) > 2 else second
    return f"""【オープニング】
今日の星の流れを、静かに読んでいきます。

【アスペクト解説】
今日は{top['planet1']}と{top['planet2']}の{top['aspect']}がタイトで、{top['keyword']}が出やすい日です。{second['planet1']}と{second['planet2']}は、勢いと判断のバランスを問いかけます。{third['planet1']}と{third['planet2']}は、気持ちの揺れを整えるヒントになります。

【今日のテーマ】
広げるより、絞る。急ぐより、整える。

【アドバイス】
大事な返事や決断は、一度メモにしてから動いてください。

【締め】
今日は、整えた分だけ前に進みやすくなります。"""


def parse_scenario_parts(script: str, *, scenario_date: date | None = None) -> ScenarioParts:
    labels = {
        "opening": "オープニング",
        "aspects": "アスペクト解説",
        "theme": "今日のテーマ",
        "advice": "アドバイス",
        "closing": "締め",
    }
    parts: dict[str, str] = {}
    for key, label in labels.items():
        pattern = rf"【{re.escape(label)}】\s*(.*?)(?=\n【|$)"
        match = re.search(pattern, script, flags=re.DOTALL)
        parts[key] = _clean_text(match.group(1)) if match else ""
    return ScenarioParts(
        date=(scenario_date or date.today()).isoformat(),
        opening=parts["opening"],
        aspects=parts["aspects"],
        theme=parts["theme"],
        advice=parts["advice"],
        closing=parts["closing"],
    )


def save_scenario(parts: ScenarioParts, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = parts.date.replace("-", "")
    txt_path = output_dir / f"scenario_{stamp}.txt"
    json_path = output_dir / f"scenario_{stamp}.json"
    txt_path.write_text(parts.full_script + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(parts.as_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"txt": txt_path, "json": json_path}


def synthesize_voicevox(
    text: str,
    *,
    output_path: Path,
    speaker: int | None = None,
    voicevox_url: str | None = None,
    timeout: int = 60,
) -> Path:
    base_url = (voicevox_url or os.getenv("VOICEVOX_URL") or DEFAULT_VOICEVOX_URL).rstrip("/")
    speaker_id = int(speaker if speaker is not None else os.getenv("VOICEVOX_SPEAKER", DEFAULT_VOICEVOX_SPEAKER))
    query_response = requests.post(
        f"{base_url}/audio_query",
        params={"text": text, "speaker": speaker_id},
        timeout=timeout,
    )
    query_response.raise_for_status()
    synthesis_response = requests.post(
        f"{base_url}/synthesis",
        params={"speaker": speaker_id},
        json=query_response.json(),
        timeout=timeout,
    )
    synthesis_response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(synthesis_response.content)
    return output_path


def create_social_video(
    *,
    image_path: Path,
    output_path: Path,
    audio_path: Path | None = None,
    duration_seconds: int | None = None,
) -> Path:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg is not installed")

    image_path = image_path.resolve()
    output_path = output_path.resolve()
    if not image_path.exists():
        raise RuntimeError(f"background image not found: {image_path}")
    if image_path.suffix.lower() not in SOCIAL_IMAGE_SUFFIXES:
        raise RuntimeError("background image must be jpg, jpeg, png, or webp")
    if audio_path is not None:
        audio_path = audio_path.resolve()
        if not audio_path.exists():
            raise RuntimeError(f"audio file not found: {audio_path}")
        if audio_path.suffix.lower() not in SOCIAL_AUDIO_SUFFIXES:
            raise RuntimeError("audio file must be wav, mp3, m4a, or aac")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    duration = int(duration_seconds or os.getenv("SOCIAL_VIDEO_DURATION_SECONDS", DEFAULT_VIDEO_DURATION_SECONDS))
    duration = max(5, min(duration, 600))
    filter_graph = (
        "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,boxblur=24:1[bg];"
        "[0:v]scale=920:1500:force_original_aspect_ratio=decrease[fg];"
        "[bg][fg]overlay=(W-w)/2:(H-h)/2,format=yuv420p[v]"
    )
    cmd = [
        ffmpeg,
        "-y",
        "-loop",
        "1",
        "-framerate",
        "30",
        "-i",
        str(image_path),
    ]
    if audio_path is not None:
        cmd.extend(["-i", str(audio_path)])
    cmd.extend(
        [
            "-filter_complex",
            filter_graph,
            "-map",
            "[v]",
        ]
    )
    if audio_path is not None:
        cmd.extend(["-map", "1:a", "-c:a", "aac", "-b:a", "192k", "-shortest"])
    else:
        cmd.extend(["-t", str(duration), "-an"])
    cmd.extend(
        [
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
    )
    completed = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=max(120, duration + 60))
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1200:]
        raise RuntimeError(f"ffmpeg video generation failed: {detail}")
    return output_path


def publish_instagram_reel(
    *,
    video_url: str,
    caption: str,
    dry_run: bool = True,
    ig_user_id: str | None = None,
    access_token: str | None = None,
) -> dict[str, Any]:
    ig_id = ig_user_id or os.getenv("INSTAGRAM_IG_USER_ID", "").strip()
    token = access_token or os.getenv("INSTAGRAM_ACCESS_TOKEN", "").strip()
    graph_version = os.getenv("META_GRAPH_API_VERSION", DEFAULT_META_GRAPH_VERSION).strip() or DEFAULT_META_GRAPH_VERSION
    payload = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "access_token": token,
    }
    if dry_run:
        return {"dry_run": True, "platform": "instagram", "ig_user_id": ig_id, "payload": {**payload, "access_token": "***"}}
    if not ig_id or not token:
        raise RuntimeError("INSTAGRAM_IG_USER_ID and INSTAGRAM_ACCESS_TOKEN are required")
    create = requests.post(f"https://graph.facebook.com/{graph_version}/{ig_id}/media", data=payload, timeout=60)
    create.raise_for_status()
    creation_id = create.json().get("id")
    if not creation_id:
        raise RuntimeError(f"Instagram media container creation failed: {create.text}")
    publish = requests.post(
        f"https://graph.facebook.com/{graph_version}/{ig_id}/media_publish",
        data={"creation_id": creation_id, "access_token": token},
        timeout=60,
    )
    publish.raise_for_status()
    return {"platform": "instagram", "creation_id": creation_id, "publish": publish.json()}


def publish_tiktok_video(
    *,
    video_url: str,
    caption: str,
    dry_run: bool = True,
    access_token: str | None = None,
    privacy_level: str | None = None,
) -> dict[str, Any]:
    token = access_token or os.getenv("TIKTOK_ACCESS_TOKEN", "").strip()
    privacy = privacy_level or os.getenv("TIKTOK_PRIVACY_LEVEL", "SELF_ONLY").strip() or "SELF_ONLY"
    payload = {
        "post_info": {
            "title": caption,
            "privacy_level": privacy,
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
            "brand_content_toggle": False,
            "brand_organic_toggle": False,
            "is_aigc": True,
        },
        "source_info": {"source": "PULL_FROM_URL", "video_url": video_url},
    }
    if dry_run:
        return {"dry_run": True, "platform": "tiktok", "payload": payload}
    if not token:
        raise RuntimeError("TIKTOK_ACCESS_TOKEN is required")
    response = requests.post(
        "https://open.tiktokapis.com/v2/post/publish/video/init/",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=UTF-8"},
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return {"platform": "tiktok", "response": response.json()}


def generate_daily_assets(
    *,
    aspects: list[dict[str, Any]] | None = None,
    target_date: date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    use_mock: bool = False,
    make_voice: bool = False,
) -> dict[str, Any]:
    scenario_date = target_date or date.today()
    source_aspects = aspects if aspects is not None else aspects_from_global_transit(scenario_date)
    script = generate_mock_scenario(source_aspects) if use_mock else generate_scenario(source_aspects)
    parts = parse_scenario_parts(script, scenario_date=scenario_date)
    paths = save_scenario(parts, output_dir=output_dir)
    result: dict[str, Any] = {"parts": parts.as_dict(), "paths": {k: str(v) for k, v in paths.items()}}
    if make_voice:
        wav_path = output_dir / f"scenario_{scenario_date.strftime('%Y%m%d')}.wav"
        result["voicevox_wav"] = str(synthesize_voicevox(parts.full_script, output_path=wav_path))
    return result


def _keyword_for(planet1: str, planet2: str, aspect: str) -> str:
    key = (planet1, planet2, aspect)
    reverse_key = (planet2, planet1, aspect)
    return ASPECT_KEYWORDS.get(key) or ASPECT_KEYWORDS.get(reverse_key) or "流れの調整・気づき"


def _build_user_prompt(aspects: list[dict[str, Any]]) -> str:
    return "\n".join(
        [
            "以下のアスペクトデータをもとに、60秒ナレーションシナリオを作成してください。",
            "全体200〜250文字を目安にしてください。",
            "orbが小さいものほど強調してください。",
            "",
            json.dumps(aspects, ensure_ascii=False, indent=2),
        ]
    )


def _extract_text(response: Any) -> str:
    chunks: list[str] = []
    for block in getattr(response, "content", []) or []:
        text = getattr(block, "text", None)
        if text:
            chunks.append(str(text))
    return "\n".join(chunks).strip()


def _clean_text(value: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", value.strip())


def _load_aspects(path: Path | None) -> list[dict[str, Any]] | None:
    if path is None:
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("aspects")
    if not isinstance(data, list):
        raise ValueError("aspects JSON must be a list or an object with aspects")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily TikTok/Instagram astrology scenario assets.")
    parser.add_argument("--date", dest="target_date", help="YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--aspects-json", type=Path, help="Path to aspects JSON. Defaults to global transit snapshot.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory.")
    parser.add_argument("--mock", action="store_true", help="Do not call Claude; use a local sample renderer.")
    parser.add_argument("--voicevox", action="store_true", help="Also synthesize WAV via a running VOICEVOX engine.")
    parser.add_argument("--video-url", help="Public video URL for optional social posting.")
    parser.add_argument("--post-instagram", action="store_true", help="Publish/dry-run Instagram Reel.")
    parser.add_argument("--post-tiktok", action="store_true", help="Publish/dry-run TikTok video.")
    parser.add_argument("--live-post", action="store_true", help="Actually call social posting APIs. Default is dry-run.")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.target_date) if args.target_date else None
    result = generate_daily_assets(
        aspects=_load_aspects(args.aspects_json),
        target_date=target_date,
        output_dir=args.out,
        use_mock=args.mock,
        make_voice=args.voicevox,
    )
    caption = result["parts"]["theme"] or result["parts"]["opening"]
    if args.post_instagram or args.post_tiktok:
        if not args.video_url:
            raise RuntimeError("--video-url is required for social posting")
        if args.post_instagram:
            result["instagram"] = publish_instagram_reel(
                video_url=args.video_url,
                caption=caption,
                dry_run=not args.live_post,
            )
        if args.post_tiktok:
            result["tiktok"] = publish_tiktok_video(
                video_url=args.video_url,
                caption=caption,
                dry_run=not args.live_post,
            )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
