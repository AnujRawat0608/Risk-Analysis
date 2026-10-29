"""
NewsAPI ingestion pipeline.

Replaces GDELT as the primary news source after GDELT's free API became
persistently unreliable (rate-limiting/connection resets lasting well
beyond their stated "5 seconds between requests" cooldown). Same overall
shape as gdelt_ingest.py: fetch articles per keyword query, ask Groq/Llama
to decide relevance + extract a structured event, store via the shared
ingestion_base helpers.

NewsAPI free tier notes:
  - 100 requests/day on the free "Developer" plan.
  - Free plan only returns articles from the last month (fine for our
    72-hour lookback window).
  - Free plan is restricted to non-production/dev use per NewsAPI's terms;
    fine for calling from your own backend script, not for a public-facing
    product without upgrading.

Run on a schedule (e.g. every few hours, given the 100/day cap) via
cron/Task Scheduler/GitHub Actions.
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
logger = logging.getLogger("newsapi_ingest")

NEWSAPI_URL = "https://newsapi.org/v2/everything"
NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
DB_DSN = os.environ["DATABASE_URL"]
GROQ_MODEL = "openai/gpt-oss-120b"
SOURCE_NAME = "NewsAPI"

# Same intent as GDELT's query list, phrased for NewsAPI's simpler keyword
# search (no OR/boolean groups needed the same way, but NewsAPI does
# support them — kept similar for consistency and easy comparison).
SEARCH_QUERIES = [
    "Russia Ukraine war",
    "Russian airspace ban carriers",
    "shipping route conflict",
    "strait blockade",
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


def fetch_newsapi_articles(query: str, page_size: int = 30) -> list[dict]:
    """NewsAPI's /v2/everything, most recent first, last-month window
    (free tier's limit) narrowed by our own event_time lookback anyway."""
    params = {
        "q": query,
        "apiKey": NEWSAPI_KEY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
    }

    for attempt in range(3):
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)

        if resp.status_code == 429:
            wait = 15 * (attempt + 1)
            logger.warning("Rate limited on '%s', waiting %ds (attempt %d/3)", query, wait, attempt + 1)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            # NewsAPI returns a JSON body with "message" even on errors
            # (bad key, quota exceeded, etc.) — log it instead of raising
            # blind, since raise_for_status() alone won't show why.
            try:
                detail = resp.json().get("message", resp.text)
            except ValueError:
                detail = resp.text
            logger.error("NewsAPI error %s for '%s': %s", resp.status_code, query, detail)
            return []

        data = resp.json()
        return data.get("articles", [])

    logger.error("Still rate limited after 3 attempts for '%s', skipping", query)
    return []


def extract_event(article: dict) -> Optional[dict]:
    """Raises on API/parse failure — caller isolates that failure to this
    one article, not the whole ingestion cycle."""
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
    # NewsAPI gives ISO 8601 directly, e.g. "2026-09-28T14:32:00Z" —
    # simpler than GDELT's compact seendate format, but still validated
    # rather than trusted blindly.
    parsed["published_at"] = article.get("publishedAt")
    return parsed


def parse_event_time(published_at: Optional[str]) -> Optional[datetime]:
    if not published_at:
        return None
    try:
        # Python's fromisoformat doesn't accept a trailing "Z" before 3.11;
        # normalize it to an explicit UTC offset so this works regardless
        # of exactly which 3.11.x patch is running.
        normalized = published_at.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        logger.warning("Could not parse publishedAt %r, falling back to insertion time", published_at)
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
    event_time = parse_event_time(parsed.get("published_at"))

    event_id = insert_event_dedup(
        cur,
        source=SOURCE_NAME,
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


"""
NewsAPI ingestion pipeline.

Replaces GDELT as the primary news source after GDELT's free API became
persistently unreliable (rate-limiting/connection resets lasting well
beyond their stated "5 seconds between requests" cooldown). Same overall
shape as gdelt_ingest.py: fetch articles per keyword query, ask Groq/Llama
to decide relevance + extract a structured event, store via the shared
ingestion_base helpers.

NewsAPI free tier notes:
  - 100 requests/day on the free "Developer" plan.
  - Free plan only returns articles from the last month (fine for our
    72-hour lookback window).
  - Free plan is restricted to non-production/dev use per NewsAPI's terms;
    fine for calling from your own backend script, not for a public-facing
    product without upgrading.

Run on a schedule (e.g. every few hours, given the 100/day cap) via
cron/Task Scheduler/GitHub Actions.
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
logger = logging.getLogger("newsapi_ingest")

NEWSAPI_URL = "https://newsapi.org/v2/everything"
NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
DB_DSN = os.environ["DATABASE_URL"]
GROQ_MODEL = "openai/gpt-oss-120b"
SOURCE_NAME = "NewsAPI"

# Same intent as GDELT's query list, phrased for NewsAPI's simpler keyword
# search (no OR/boolean groups needed the same way, but NewsAPI does
# support them — kept similar for consistency and easy comparison).
SEARCH_QUERIES = [
    "Russia Ukraine war",
    "Russian airspace ban carriers",
    "shipping route conflict",
    "strait blockade",
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


def fetch_newsapi_articles(query: str, page_size: int = 30) -> list[dict]:
    """NewsAPI's /v2/everything, most recent first, last-month window
    (free tier's limit) narrowed by our own event_time lookback anyway."""
    params = {
        "q": query,
        "apiKey": NEWSAPI_KEY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
    }

    for attempt in range(3):
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)

        if resp.status_code == 429:
            wait = 15 * (attempt + 1)
            logger.warning("Rate limited on '%s', waiting %ds (attempt %d/3)", query, wait, attempt + 1)
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            # NewsAPI returns a JSON body with "message" even on errors
            # (bad key, quota exceeded, etc.) — log it instead of raising
            # blind, since raise_for_status() alone won't show why.
            try:
                detail = resp.json().get("message", resp.text)
            except ValueError:
                detail = resp.text
            logger.error("NewsAPI error %s for '%s': %s", resp.status_code, query, detail)
            return []

        data = resp.json()
        return data.get("articles", [])

    logger.error("Still rate limited after 3 attempts for '%s', skipping", query)
    return []


def extract_event(article: dict) -> Optional[dict]:
    """Raises on API/parse failure — caller isolates that failure to this
    one article, not the whole ingestion cycle."""
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
    # NewsAPI gives ISO 8601 directly, e.g. "2026-09-28T14:32:00Z" —
    # simpler than GDELT's compact seendate format, but still validated
    # rather than trusted blindly.
    parsed["published_at"] = article.get("publishedAt")
    return parsed


def parse_event_time(published_at: Optional[str]) -> Optional[datetime]:
    if not published_at:
        return None
    try:
        # Python's fromisoformat doesn't accept a trailing "Z" before 3.11;
        # normalize it to an explicit UTC offset so this works regardless
        # of exactly which 3.11.x patch is running.
        normalized = published_at.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        logger.warning("Could not parse publishedAt %r, falling back to insertion time", published_at)
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
    event_time = parse_event_time(parsed.get("published_at"))

    event_id = insert_event_dedup(
        cur,
        source=SOURCE_NAME,
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
                    articles = fetch_newsapi_articles(query)
                except requests.RequestException as e:
                    logger.error("NewsAPI fetch failed for query '%s': %s", query, e)
                    continue

                time.sleep(1)  # NewsAPI's free tier is per-day, not per-second,
                                # but a small gap is still polite/safe

                for article in articles:
                    try:
                        parsed = extract_event(article)
                    except Exception as e:
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

if __name__ == "__main__":
    run_ingestion_cycle()