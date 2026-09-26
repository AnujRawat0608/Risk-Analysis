"""
Weather ingestion: free, no API key.

Source: Open-Meteo Marine API (https://marine-api.open-meteo.com). Global
coverage, no signup, no rate-limit key required for reasonable use.

Unlike GDELT, this doesn't need an LLM call at all: wave height and wind
speed are numbers, and "how bad is a 6m wave" is a fixed lookup table, not
something worth spending a Groq call on. That makes this source cheaper and
more reliable than the news-based ones — nothing to hallucinate or
misparse. Only maritime chokepoints are checked; weather isn't a meaningful
signal for rail/road/air chokepoints in this model.

Run on a schedule (e.g. every 1-3 hours — sea state doesn't change as fast
as news does, so this doesn't need GDELT's 15-30 min cadence).
"""

from dotenv import load_dotenv
load_dotenv()

import os
import logging
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras

from .ingestion_base import insert_event_dedup, link_event_to_chokepoint, record_source_health

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_ingest")

DB_DSN = os.environ["DATABASE_URL"]
OPEN_METEO_MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
SOURCE_NAME = "OPEN_METEO"

# Wave height (meters) -> severity. These are deliberately conservative
# starting thresholds for open-water chokepoint transit, not small-craft
# advisories — tune once you see real data across a few storm events.
WAVE_HEIGHT_SEVERITY = [
    (6.0, 5),  # >= 6m: severe sea state, major routing risk
    (4.5, 4),
    (3.0, 3),
    (2.0, 2),
    (0.0, 0),  # below 2m: not worth logging as an event
]

# Below this severity, we don't bother storing an event — a calm-seas
# reading every few hours for every chokepoint would just be noise.
MIN_SEVERITY_TO_STORE = 2


def _severity_for_wave_height(meters: float) -> int:
    for threshold, severity in WAVE_HEIGHT_SEVERITY:
        if meters >= threshold:
            return severity
    return 0


def fetch_marine_conditions(lat: float, lon: float) -> dict:
    """Current + next-24h max wave height and wind wave height for a point."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "wave_height,wind_wave_height",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_MARINE_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def _max_wave_height(payload: dict) -> float:
    heights = payload.get("hourly", {}).get("wave_height", []) or []
    heights = [h for h in heights if h is not None]
    return max(heights) if heights else 0.0


def run_ingestion_cycle():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    stored = 0
    had_error = None

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat
                FROM chokepoints WHERE mode = 'maritime'
                """
            )
            chokepoints = cur.fetchall()

            for cp in chokepoints:
                try:
                    payload = fetch_marine_conditions(cp["lat"], cp["lon"])
                except requests.RequestException as e:
                    logger.warning("Marine fetch failed for %s: %s", cp["name"], e)
                    continue

                max_height = _max_wave_height(payload)
                severity = _severity_for_wave_height(max_height)
                if severity < MIN_SEVERITY_TO_STORE:
                    continue

                # Dedup key: one weather event per chokepoint per UTC hour,
                # so a cycle that runs every hour doesn't re-insert the same
                # reading, but a genuinely worsening sea state each hour
                # still gets its own row.
                hour_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
                source_ref = f"open-meteo:{cp['id']}:{hour_bucket}"

                event_id = insert_event_dedup(
                    cur,
                    source=SOURCE_NAME,
                    source_ref=source_ref,
                    event_type="weather",
                    headline=f"Elevated sea state near {cp['name']}",
                    summary=f"Forecast max wave height {max_height:.1f}m in next 24h.",
                    severity=severity,
                    confidence=0.9,  # numeric forecast data, high confidence in the reading itself
                    raw_payload=payload,
                )
                if event_id:
                    link_event_to_chokepoint(cur, event_id, cp["id"])
                    stored += 1
                    logger.info("Stored weather event for %s: %.1fm -> severity %d", cp["name"], max_height, severity)

            record_source_health(cur, SOURCE_NAME, success=True, items_ingested=stored)
            conn.commit()
    except Exception as e:
        had_error = str(e)
        conn.rollback()
        with conn.cursor() as cur:
            record_source_health(cur, SOURCE_NAME, success=False, error=had_error)
            conn.commit()
        raise
    finally:
        conn.close()

    logger.info("Weather ingestion cycle complete. %d events stored.", stored)
    return stored


if __name__ == "__main__":
    run_ingestion_cycle()