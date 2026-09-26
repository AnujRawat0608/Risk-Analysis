"""
Shared plumbing for every ingestion source (GDELT, weather, sanctions, ...).

Each source has its own fetch + interpret logic, but they all share the same
three failure-prone steps: writing an event without duplicating it, keeping
one bad value from violating the DB's own constraints, and recording whether
the source is actually healthy. Putting those three things here once means
a new free-data source is "write a fetch function," not "re-solve dedup and
error-handling from scratch."
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("ingestion_base")

VALID_EVENT_TYPES = {
    "conflict", "blockade", "sanctions", "strike",
    "piracy", "weather", "congestion", "regulatory", "other",
}


def clamp_severity(value) -> int:
    """Event.severity has a DB CHECK (1-5). Never trust an upstream value
    (an LLM extraction or a third-party feed) to already satisfy that."""
    try:
        v = int(value)
    except (TypeError, ValueError):
        return 1
    return max(1, min(5, v))


def clamp_confidence(value) -> float:
    """Event.confidence has a DB CHECK (0-1)."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.5
    return max(0.0, min(1.0, v))


def clamp_event_type(value: Optional[str]) -> str:
    if value in VALID_EVENT_TYPES:
        return value
    return "other"


def insert_event_dedup(cur, *, source: str, source_ref: Optional[str], event_type: str,
                        headline: str, summary: Optional[str], severity, confidence,
                        event_time: Optional[datetime] = None, raw_payload: Optional[dict] = None) -> Optional[str]:
    """
    Insert an event, skipping it if source_ref has already been stored
    (requires migration 002's UNIQUE constraint on events.source_ref).

    Returns the new event's id, or None if it was a duplicate (or source_ref
    was falsy and you're relying on some other dedup strategy upstream).
    """
    import json

    cur.execute(
        """
        INSERT INTO events (source, source_ref, event_type, headline, summary,
                             severity, confidence, event_time, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_ref) DO NOTHING
        RETURNING id
        """,
        (
            source,
            source_ref,
            clamp_event_type(event_type),
            headline,
            summary,
            clamp_severity(severity),
            clamp_confidence(confidence),
            event_time or datetime.now(timezone.utc),
            json.dumps(raw_payload, default=str) if raw_payload is not None else None,
        ),
    )
    row = cur.fetchone()
    return row[0] if row else None


def link_event_to_chokepoint(cur, event_id: str, chokepoint_id: Optional[str], impact_note: Optional[str] = None):
    if not event_id or not chokepoint_id:
        return
    cur.execute(
        """
        INSERT INTO event_chokepoints (event_id, chokepoint_id, impact_note)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (event_id, chokepoint_id, impact_note),
    )


def record_source_health(cur, source: str, *, success: bool, error: Optional[str] = None, items_ingested: int = 0):
    """
    Call once per ingestion run per source. Powers a simple health check:
    `SELECT * FROM source_health WHERE consecutive_failures > 2` tells you
    a free API has started failing before a person notices scores going stale.
    """
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO source_health (source, last_success_at, last_attempt_at, last_error,
                                    consecutive_failures, items_ingested_last_run)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source) DO UPDATE SET
            last_attempt_at = EXCLUDED.last_attempt_at,
            last_success_at = CASE WHEN %s THEN EXCLUDED.last_success_at
                                    ELSE source_health.last_success_at END,
            last_error = EXCLUDED.last_error,
            consecutive_failures = CASE WHEN %s THEN 0
                                        ELSE source_health.consecutive_failures + 1 END,
            items_ingested_last_run = EXCLUDED.items_ingested_last_run
        """,
        (source, now if success else None, now, error, 0 if success else 1, items_ingested,
         success, success),
    )