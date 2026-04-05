from __future__ import annotations

import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

router = APIRouter()


def _parse_target_date(date_str: str | None):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@router.post("/transit/single", response_class=JSONResponse)
async def transit_single(request: Request):
    try:
        from services.transit_calc import calc_transits_single
        from services.western_calc import calc_western_from_payload

        body = await request.json()
        payload = body.get("payload", {})
        target_date = _parse_target_date(body.get("date"))

        natal_result = calc_western_from_payload(payload)
        natal_planets = natal_result.get("planets", [])

        lat = float(payload.get("lat", 35.6895))
        lng = float(payload.get("lng", payload.get("lon", 139.6917)))

        transit = calc_transits_single(natal_planets, target_date=target_date, lat=lat, lng=lng)
        transit["natal_summary"] = [
            {"name": p["name"], "sign": p["sign"], "degree": round(p["degree"], 2)}
            for p in natal_planets
        ]
        return JSONResponse(content=transit)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/transit/synastry", response_class=JSONResponse)
async def transit_synastry(request: Request):
    try:
        from services.transit_calc import calc_transits_synastry
        from services.western_calc import calc_western_from_payload

        body = await request.json()
        payload_a = body.get("payload_a", {})
        payload_b = body.get("payload_b", {})
        target_date = _parse_target_date(body.get("date"))

        natal_a = calc_western_from_payload(payload_a).get("planets", [])
        natal_b = calc_western_from_payload(payload_b).get("planets", [])

        lat = float(payload_a.get("lat", 35.6895))
        lng = float(payload_a.get("lng", payload_a.get("lon", 139.6917)))

        result = calc_transits_synastry(natal_a, natal_b, target_date=target_date, lat=lat, lng=lng)
        return JSONResponse(content=result)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(content={"text": f"（サーバーエラー: {e}）"}, status_code=200)


@router.post("/transit/interpret", response_class=JSONResponse)
async def transit_interpret(request: Request):
    try:
        body = await request.json()
        long_term = body.get("long_term", [])
        today_aspects = body.get("today_aspects", [])
        natal_planets = body.get("natal_planets", [])

        major = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"]
        natal_lines = []
        for p in natal_planets:
            if p.get("name") in major:
                natal_lines.append(f"{p['name']}: {p.get('sign', '')} {p.get('house', '')}ハウス")
        natal_summary = " / ".join(natal_lines) if natal_lines else "（データなし）"

        asp_jp = {
            "conjunction": "が重なる",
            "opposition": "が向き合う",
            "square": "が摩擦を生む",
            "trine": "が流れをつくる",
            "sextile": "がチャンスをつくる",
        }
        planet_jp = {
            "Saturn": "土星（責任・制約・成熟）",
            "Jupiter": "木星（拡大・幸運・成長）",
            "Uranus": "天王星（変化・革新・解放）",
            "Neptune": "海王星（夢・直感・混乱）",
            "Pluto": "冥王星（変容・再生・深化）",
        }
        natal_jp = {
            "Sun": "あなた自身の核",
            "Moon": "感情・安心感",
            "Mercury": "思考・コミュニケーション",
            "Venus": "愛・価値観・お金",
            "Mars": "行動力・意欲",
            "Jupiter": "拡大・幸運のポイント",
            "Saturn": "責任・試練のポイント",
            "ASC": "自己表現・第一印象",
            "MC": "社会的な方向性・仕事",
            "North Node": "魂の方向性",
        }

        lt_lines = []
        for item in long_term[:15]:
            tp = planet_jp.get(item.get("transit_planet", ""), item.get("transit_planet", ""))
            asp = asp_jp.get(item.get("aspect", ""), item.get("aspect", ""))
            np_label = natal_jp.get(item.get("natal_planet", ""), item.get("natal_planet", ""))
            start = item.get("start_date", "")[:7].replace("-", "年", 1).replace("-", "月")
            end = item.get("end_date", "")[:7].replace("-", "年", 1).replace("-", "月")
            status = {"active": "進行中", "upcoming": "まもなく", "past": "終了"}.get(item.get("status", ""), "")
            lt_lines.append(f"[{status}] {start}〜{end}: {tp}{asp}（{np_label}）")
        long_term_summary = "\n".join(lt_lines) if lt_lines else "（データなし）"

        today_lines = []
        for aspect in today_aspects[:7]:
            tp = aspect.get("transit_planet", "")
            np_label = natal_jp.get(aspect.get("natal_planet", ""), aspect.get("natal_planet", ""))
            asp = asp_jp.get(aspect.get("aspect", ""), aspect.get("aspect", ""))
            today_lines.append(f"{planet_jp.get(tp, tp)}{asp}（{np_label}）orb{aspect.get('orb', 0):.1f}°")
        today_summary = "\n".join(today_lines) if today_lines else "（データなし）"

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "transit_interpret.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")
        prompt = tpl.format(
            common_rules=common_rules,
            natal_summary=natal_summary,
            long_term_summary=long_term_summary,
            today_summary=today_summary,
        )

        try:
            from services.ai_report import generate_report as _gen

            _astro = {
                "_meta": {
                    "output_style": "web",
                    "detail_level": "standard",
                    "astrology_system": "western",
                    "user_name": "",
                    "display_name": "",
                    "birth_date": "",
                    "today": "",
                    "age_years": "",
                    "theme": "overall",
                    "message": prompt,
                    "observations_text": "",
                }
            }
            text = _gen(_astro, style="web", report_type="raw_prompt")
            if not text or "生成エラー" in text or text.startswith("GEMINI"):
                text = f"（AI生成エラー: {text}）"
        except Exception as ai_err:
            text = f"（AI生成エラー: {ai_err}）"

        return JSONResponse(content={"text": text})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))
