"""
GDELT ingestion pipeline.

Pulls recent global event data from GDELT's free DOC 2.0 API, filters for
supply-chain-relevant keywords, and uses Groq (Llama) to:
  1. Decide whether an article is actually relevant to a tracked chokepoint
  2. Extract a structured event record (type, severity, affected chokepoint, summary)

Run on a schedule (e.g. every 15-30 min) via cron/Task Scheduler.

Changes from v1:
  - Uses insert_event_dedup (ingestion_base) so the same real-world event
    matched by more than one overlapping search query no longer gets
    inserted — and therefore scored — multiple times.
  - Each article's extraction is wrapped individually so one malformed LLM
    response or transient API error can't abort the rest of the cycle.
  - Reports success/failure to source_health so a silent GDELT outage or
    schema change shows up on a dashboard instead of just going quiet.
  - Chokepoint matching is now an exact lookup against a fixed list of
    names given to the LLM, rather than a free-text guess fuzzy-matched
    via ILIKE — removes an entire class of silent "extracted but never
    linked to any chokepoint" events.
  - event_time is now parsed from GDELT's own `seendate` field (the
    article's actual publish time) instead of defaulting to insertion
    time. Without this, every event's age is measured from when you
    happened to run the script, not from when the event actually
    occurred — which silently breaks risk_scoring.py's 72-hour lookback
    window the moment more than 72 hours pass between ingestion runs.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

from groq import Groq
import psycopg2
import psycopg2.extras
import time

from .ingestion_base import insert_event_dedup, link_event_to_chokepoint, record_source_health

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdelt_ingest")

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
DB_DSN = os.environ["DATABASE_URL"]
GROQ_MODEL = "llama-3.3-70b-versatile"
SOURCE_NAME = "GDELT"

SEARCH_QUERIES = [
    "strait blockade OR closure",
    "canal disruption OR attack",
    "shipping route war OR conflict",
    "port strike OR closure",
    "sanctions trade export",
    "airspace closure NOTAM",
    "border crossing closed trade",
    "houthi shipping attack",
    "drought canal draft restriction",
    "Russia Ukraine war",
    "Russian airspace ban carriers",
    "EU sanctions Russia aviation",
]

client = Groq()  # picks up GROQ_API_KEY from env

CHOKEPOINT_NAMES = [
    "Strait of Hormuz",
    "Suez Canal",
    "Strait of Malacca",
    "Bab-el-Mandeb Strait",
    "Panama Canal",
    "Taiwan Strait",
    "Black Sea Corridor",
    "Russian Airspace Corridor",
    "China-Europe Rail (Kazakhstan corridor)",
    "US-Mexico Border (Laredo)",
]

EXTRACTION_SYSTEM_PROMPT = f"""You are a supply chain risk analyst. You will be given a news \
article headline and URL. Determine if it describes a real event affecting global trade \
routes, shipping lanes, airspace, or land/rail freight corridors.

Respond ONLY with a JSON object with this exact shape:
{{
  "is_relevant": boolean,
  "event_type": "conflict" | "blockade" | "sanctions" | "strike" | "piracy" | "weather" | "congestion" | "regulatory" | "other",
  "severity": integer 1-5 (1=minor/local, 5=critical/major route closure),
  "confidence": float 0-1,
  "likely_chokepoint": one of {json.dumps(CHOKEPOINT_NAMES)} or null,
  "summary": string
}}

Only set likely_chokepoint to one of the exact strings in that list, or null if none clearly \
apply. Do not invent a name not in the list. If is_relevant is false, you may leave other \
fields as null/0. Be conservative: only mark high severity (4-5) for events that plausibly \
close or seriously constrain a route, not routine political friction."""


def fetch_gdelt_articles(query: str, max_records: int = 50) -> list[dict]:
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": max_records,
        "timespan": "1d",
        "sort": "datedesc",
    }

    for attempt in range(3):
        resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
        if resp.status_code == 429:
            wait = 10 * (attempt + 1)  # 10s, then 20s, then 30s
            logger.warning("Rate limited on '%s', waiting %ds (attempt %d/3)", query, wait, attempt + 1)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])

    logger.error("Still rate limited after 3 attempts for '%s', skipping", query)
    return []


def extract_event(article: dict) -> Optional[dict]:
    """Raises on API/parse failure — caller is responsible for isolating
    that failure to this one article, not the whole ingestion cycle."""
    headline = article.get("title", "")
    url = article.get("url", "")

    completion = client.chat.completions.create(
        model=GROQ_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
            {"role": "user", "content": f"Headline: {headline}\nURL: {url}"},
        ],
        max_tokens=500,
    )

    text = completion.choices[0].message.content.strip()
    parsed = json.loads(text)  # let json.JSONDecodeError propagate to caller

    if not parsed.get("is_relevant"):
        return None

    parsed["headline"] = headline
    parsed["source_ref"] = url
    # GDELT's own publish timestamp for the article, e.g. "20260924T153000Z".
    # Captured here (not computed later) so raw_payload also preserves it.
    parsed["seendate"] = article.get("seendate")
    return parsed


def parse_event_time(seendate: Optional[str]) -> Optional[datetime]:
    """GDELT's seendate is typically 'YYYYMMDDTHHMMSSZ'. Falls back to None
    (caller then defaults to now()) if missing or in an unexpected format —
    a parsing surprise here must not crash the whole ingestion cycle."""
    if not seendate:
        return None
    try:
        return datetime.strptime(seendate, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.warning("Could not parse seendate %r, falling back to insertion time", seendate)
        return None


def match_chokepoint_id(cur, chokepoint_name: Optional[str]) -> Optional[str]:
    if not chokepoint_name:
        return None
    cur.execute("SELECT id FROM chokepoints WHERE name = %s", (chokepoint_name,))
    row = cur.fetchone()
    return row[0] if row else None


def store_event(cur, parsed: dict) -> tuple[Optional[str], Optional[str]]:
    """Returns (event_id, chokepoint_id). event_id is None if this
    source_ref was already stored (duplicate across overlapping queries)."""
    event_time = parse_event_time(parsed.get("seendate"))

    event_id = insert_event_dedup(
        cur,
        source="GDELT",
        source_ref=parsed.get("source_ref"),
        event_type=parsed.get("event_type") or "other",
        headline=parsed["headline"],
        summary=parsed.get("summary"),
        severity=parsed.get("severity") or 1,
        confidence=parsed.get("confidence") or 0.5,
        event_time=event_time,
        raw_payload=parsed,
    )
    if not event_id:
        return None, None

    chokepoint_id = match_chokepoint_id(cur, parsed.get("likely_chokepoint"))
    link_event_to_chokepoint(cur, event_id, chokepoint_id)
    return event_id, chokepoint_id


def run_ingestion_cycle():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    total_stored = 0
    total_duplicates = 0
    total_extract_failures = 0
    had_error = None

    try:
        with conn.cursor() as cur:
            for query in SEARCH_QUERIES:
                try:
                    articles = fetch_gdelt_articles(query)
                except requests.RequestException as e:
                    logger.error("GDELT fetch failed for query '%s': %s", query, e)
                    continue

                time.sleep(6)  # be polite to GDELT's free API between queries

                for article in articles:
                    try:
                        parsed = extract_event(article)
                    except Exception as e:
                        # A malformed model response or a Groq hiccup on one
                        # article must not abort the rest of the batch.
                        total_extract_failures += 1
                        logger.warning("extract_event failed for %r: %s", article.get("title"), e)
                        continue

                    if not parsed:
                        continue

                    event_id, chokepoint_id = store_event(cur, parsed)
                    if event_id:
                        total_stored += 1
                        logger.info(
                            "Stored event %s (severity=%s, chokepoint=%s)",
                            event_id, parsed.get("severity"), chokepoint_id,
                        )
                    else:
                        total_duplicates += 1
                conn.commit()

            record_source_health(cur, SOURCE_NAME, success=True, items_ingested=total_stored)
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

    logger.info(
        "Ingestion cycle complete. %d stored, %d duplicates skipped, %d extraction failures.",
        total_stored, total_duplicates, total_extract_failures,
    )
    return total_stored


if __name__ == "__main__":
    run_ingestion_cycle()