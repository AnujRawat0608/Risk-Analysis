"""
Risk scoring service.

Rolls up recent events per chokepoint into a 0-100 risk score with a
green/yellow/red status. Run this after each ingestion cycle (or on its
own schedule, e.g. every 15 min) so risk_scores stays current.

Changes from v1:
  - decay_weight no longer produces a weight > 1 for a slightly future-dated
    event (clock skew, timezone bug in an ingestion source, etc.)
  - LOOKBACK_HOURS is interpolated directly since it's a trusted module
    constant, not user input — avoids relying on psycopg2's int-adaptation
    happening to land correctly inside the existing quoted interval string.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import math
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("risk_scoring")

DB_DSN = os.environ["DATABASE_URL"]

LOOKBACK_HOURS = 72
HALF_LIFE_HOURS = 24


def decay_weight(event_time: datetime, now: datetime) -> float:
    age_hours = max(0.0, (now - event_time).total_seconds() / 3600)
    return math.pow(0.5, age_hours / HALF_LIFE_HOURS)


def status_for_score(score: float) -> str:
    if score >= 67:
        return "red"
    if score >= 34:
        return "yellow"
    return "green"


def compute_scores():
    conn = psycopg2.connect(DB_DSN)
    now = datetime.now(timezone.utc)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name FROM chokepoints")
            chokepoints = cur.fetchall()

            for cp in chokepoints:
                cur.execute(
                    f"""
                    SELECT e.id, e.severity, e.confidence, e.event_time
                    FROM events e
                    JOIN event_chokepoints ec ON ec.event_id = e.id
                    WHERE ec.chokepoint_id = %s
                      AND e.event_time > now() - interval '{LOOKBACK_HOURS} hours'
                    """,
                    (cp["id"],),
                )
                events = cur.fetchall()

                if not events:
                    score = 0.0
                    driving_ids = []
                else:
                    contributions = []
                    for ev in events:
                        w = decay_weight(ev["event_time"], now)
                        severity = ev["severity"] or 1
                        confidence = float(ev["confidence"] or 0.5)
                        contribution = (severity ** 1.5) * confidence * w
                        contributions.append((ev["id"], contribution))

                    score = min(100.0, sum(c for _, c in contributions) * 4)
                    driving_ids = [
                        eid for eid, _ in sorted(contributions, key=lambda x: -x[1])[:3]
                    ]

                status = status_for_score(score)

                cur.execute(
                    """
                    INSERT INTO risk_scores (chokepoint_id, score, status, driving_event_ids)
                    VALUES (%s, %s, %s, %s::uuid[])
                    """,
                    (cp["id"], round(score, 2), status, driving_ids),
                )

                logger.info("Chokepoint '%s' -> score=%.1f status=%s", cp["name"], score, status)

            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    compute_scores()