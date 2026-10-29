"""
GDELT ingestion pipeline.

Pulls recent global event data from GDELT's free DOC 2.0 API, filters for
supply-chain-relevant keywords, and uses Groq (Llama) to:
  1. Decide whether an article is actually relevant to a tracked chokepoint
  2. Extract a structured event record (type, severity, affected chokepoint, summary)

Run this once or twice a day (not every few minutes): chokepoint risk doesn't change
by the minute, and both GDELT and Groq's free tier rate-limit hard.

Changes from v1, all aimed at staying inside the free limits:
  - GDELT asks for at most one request every 5 seconds. Queries are now spaced 6s apart
    (v1 used 2s, which triggered 429s from the second query onwards).
  - A 429 honours the Retry-After header, and the run stops after two queries in a row
    are rate limited instead of hammering a service that has already said no.
  - Articles whose URL is already stored are skipped BEFORE calling Groq, so repeated
    runs don't re-send the same headlines (saves the daily token budget and stops the
    same article being counted twice in the risk score).
  - event_time is the article's own publish time (GDELT seendate), not "now", so the
    72-hour window and the decay in risk_scoring.py work on real ages.
  - A Groq rate-limit error ends the run cleanly, keeping what was already stored.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

from groq import Groq, RateLimitError
import psycopg2
import psycopg2.extras
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gdelt_ingest")

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
DB_DSN = os.environ["DATABASE_URL"]
GROQ_MODEL = "llama-3.3-70b-versatile"

SECONDS_BETWEEN_QUERIES = 6          # GDELT: no more than one request per 5 seconds
MAX_ARTICLES_PER_QUERY = 25          # fewer articles = fewer Groq calls
MAX_CONSECUTIVE_RATE_LIMITS = 2      # stop the run after this many rate-limited queries in a row

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
]

client = Groq()  # picks up GROQ_API_KEY from env

EXTRACTION_SYSTEM_PROMPT = """You are a supply chain risk analyst. You will be given a news \
article headline and URL. Determine if it describes a real event affecting global trade \
routes, shipping lanes, airspace, or land/rail freight corridors.

Respond ONLY with a JSON object with this exact shape:
{
  "is_relevant": boolean,
  "event_type": "conflict" | "blockade" | "sanctions" | "strike" | "piracy" | "weather" | "congestion" | "regulatory" | "other",
  "severity": integer 1-5 (1=minor/local, 5=critical/major route closure),
  "confidence": float 0-1,
  "likely_chokepoint": string or null,
  "summary": string
}

If is_relevant is false, you may leave other fields as null/0. Be conservative: only mark \
high severity (4-5) for events that plausibly close or seriously constrain a route, not routine \
political friction."""


class GdeltRateLimited(Exception):
    """GDELT kept answering 429 after all retries for one query."""


def fetch_gdelt_articles(query: str, max_records: int = MAX_ARTICLES_PER_QUERY) -> list[dict]:
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": max_records,
        "timespan": "1d",
        "sort": "datedesc",
    }

    for attempt in range(1, 4):
        resp = requests.get(GDELT_DOC_API, params=params, timeout=30)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "")
            wait = int(retry_after) if retry_after.isdigit() else 15 * attempt
            logger.warning("Rate limited on '%s', waiting %ds (attempt %d/3)", query, wait, attempt)
            time.sleep(wait)
            continue

        resp.raise_for_status()

        # GDELT answers some problems (e.g. a query that is too short) with a plain-text
        # message and a 200 status, which is not JSON.
        try:
            data = resp.json()
        except ValueError:
            logger.warning("GDELT returned non-JSON for '%s': %s", query, resp.text[:200].replace("\n", " "))
            return []
        return data.get("articles", [])

    raise GdeltRateLimited(query)


def article_time(article: dict) -> datetime:
    """GDELT's `seendate` looks like 20261029T123000Z. Falls back to now if it can't be read."""
    seen = article.get("seendate")
    if seen:
        try:
            return datetime.strptime(seen, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def already_stored(cur, url: str) -> bool:
    if not url:
        return True  # nothing to key on, so don't risk duplicates
    cur.execute("SELECT 1 FROM events WHERE source = 'GDELT' AND source_ref = %s LIMIT 1", (url,))
    return cur.fetchone() is not None


def extract_event(article: dict) -> Optional[dict]:
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

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse Groq output for article: %s", headline)
        return None

    if not parsed.get("is_relevant"):
        return None

    parsed["headline"] = headline
    parsed["source_ref"] = url
    parsed["event_time"] = article_time(article)
    return parsed


def match_chokepoint_id(cur, chokepoint_name: Optional[str]) -> Optional[str]:
    if not chokepoint_name:
        return None
    cur.execute(
        "SELECT id FROM chokepoints WHERE name ILIKE %s LIMIT 1",
        (f"%{chokepoint_name}%",),
    )
    row = cur.fetchone()
    return row[0] if row else None


def store_event(cur, parsed: dict):
    cur.execute(
        """
        INSERT INTO events (source, source_ref, event_type, headline, summary,
                             severity, confidence, event_time, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            "GDELT",
            parsed.get("source_ref"),
            parsed.get("event_type") or "other",
            parsed["headline"],
            parsed.get("summary"),
            parsed.get("severity") or 1,
            parsed.get("confidence") or 0.5,
            parsed.get("event_time") or datetime.now(timezone.utc),
            json.dumps(parsed, default=str),
        ),
    )
    event_id = cur.fetchone()[0]

    chokepoint_id = match_chokepoint_id(cur, parsed.get("likely_chokepoint"))
    if chokepoint_id:
        cur.execute(
            "INSERT INTO event_chokepoints (event_id, chokepoint_id) VALUES (%s, %s)",
            (event_id, chokepoint_id),
        )
    return event_id, chokepoint_id


def run_ingestion_cycle():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    total_stored = 0
    total_skipped_known = 0
    consecutive_rate_limits = 0

    try:
        with conn.cursor() as cur:
            for i, query in enumerate(SEARCH_QUERIES):
                if i > 0:
                    time.sleep(SECONDS_BETWEEN_QUERIES)  # GDELT: one request per 5 seconds

                try:
                    articles = fetch_gdelt_articles(query)
                    consecutive_rate_limits = 0
                except GdeltRateLimited:
                    consecutive_rate_limits += 1
                    logger.error("Still rate limited after 3 attempts for '%s', skipping", query)
                    if consecutive_rate_limits >= MAX_CONSECUTIVE_RATE_LIMITS:
                        logger.error(
                            "GDELT rate limited %d queries in a row. Stopping this run; "
                            "try again in an hour or so.",
                            consecutive_rate_limits,
                        )
                        break
                    continue
                except requests.RequestException as e:
                    logger.error("GDELT fetch failed for query '%s': %s", query, e)
                    continue

                logger.info("Query '%s': %d articles", query, len(articles))

                for article in articles:
                    if already_stored(cur, article.get("url", "")):
                        total_skipped_known += 1
                        continue

                    try:
                        parsed = extract_event(article)
                    except RateLimitError:
                        logger.error("Groq rate limit reached. Stopping this run; keeping what is stored.")
                        conn.commit()
                        logger.info(
                            "Ingestion cycle stopped early. %d events stored, %d already known.",
                            total_stored, total_skipped_known,
                        )
                        return total_stored

                    if parsed:
                        event_id, chokepoint_id = store_event(cur, parsed)
                        total_stored += 1
                        logger.info(
                            "Stored event %s (severity=%s, chokepoint=%s)",
                            event_id, parsed.get("severity"), chokepoint_id,
                        )
                conn.commit()
    finally:
        conn.close()

    logger.info(
        "Ingestion cycle complete. %d events stored, %d already known.",
        total_stored, total_skipped_known,
    )
    return total_stored


if __name__ == "__main__":
    run_ingestion_cycle()