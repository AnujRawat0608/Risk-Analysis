"""
Procurement integration bridge.

Answers one focused question: "given a chip order shipping from region A
to region B, what's the route risk?" — without the procurement system
needing to know anything about chokepoints, GDELT, or risk scoring internals.
"""

import os
from typing import Optional

import psycopg2
import psycopg2.extras

DB_DSN = os.environ["DATABASE_URL"]

# Known chip supply origins -> the chokepoints a shipment from that region
# typically transits. Extend this as you onboard suppliers from other regions.
SUPPLIER_REGION_ROUTES = {
    "taiwan": ["Taiwan Strait", "Strait of Malacca"],
    "south korea": ["Taiwan Strait"],
    "china": ["Taiwan Strait", "Strait of Malacca"],
    "japan": ["Taiwan Strait"],
    "netherlands": ["Suez Canal"],
    "united states": ["Panama Canal"],
    "malaysia": ["Strait of Malacca"],
    "singapore": ["Strait of Malacca"],
}


def _get_conn():
    return psycopg2.connect(DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)


def assess_shipment_risk(supplier_region: str, destination_region: Optional[str] = None) -> dict:
    key = supplier_region.strip().lower()
    chokepoint_names = SUPPLIER_REGION_ROUTES.get(key)

    if not chokepoint_names:
        return {
            "supplier_region": supplier_region,
            "known_route": False,
            "overall_status": "unknown",
            "message": f"No mapped route for '{supplier_region}'. Add it to SUPPLIER_REGION_ROUTES.",
            "chokepoints": [],
        }

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            results = []
            for name in chokepoint_names:
                cur.execute(
                    """
                    SELECT c.name, rs.score, rs.status, rs.computed_at
                    FROM chokepoints c
                    LEFT JOIN LATERAL (
                        SELECT score, status, computed_at FROM risk_scores
                        WHERE chokepoint_id = c.id ORDER BY computed_at DESC LIMIT 1
                    ) rs ON true
                    WHERE c.name = %s
                    """,
                    (name,),
                )
                row = cur.fetchone()
                if row:
                    results.append(row)

            cur.execute(
                """
                SELECT e.headline, e.summary, e.severity, e.event_time, c.name AS chokepoint_name
                FROM events e
                JOIN event_chokepoints ec ON ec.event_id = e.id
                JOIN chokepoints c ON c.id = ec.chokepoint_id
                WHERE c.name = ANY(%s)
                ORDER BY e.event_time DESC LIMIT 5
                """,
                (chokepoint_names,),
            )
            driving_events = cur.fetchall()
    finally:
        conn.close()

    status_rank = {"green": 0, "yellow": 1, "red": 2, None: 0}
    overall_status = max((r["status"] for r in results), key=lambda s: status_rank.get(s, 0), default="unknown")

    return {
        "supplier_region": supplier_region,
        "destination_region": destination_region,
        "known_route": True,
        "overall_status": overall_status,
        "chokepoints": results,
        "driving_events": driving_events,
        "recommendation": _recommendation_for_status(overall_status),
    }


def _recommendation_for_status(status: str) -> str:
    return {
        "green": "No significant route disruption detected. Proceed as planned.",
        "yellow": "Elevated risk on part of this route. Consider buffer lead time or a secondary supplier as backup.",
        "red": "Active disruption likely affecting this route. Recommend reviewing alternate suppliers/routes before confirming order timeline.",
        "unknown": "Route risk data not available for this lane.",
    }.get(status, "Route risk data not available for this lane.")