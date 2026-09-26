"""
Route risk API.

The single entry point other systems (your procurement product, this
RoutingPage frontend, anything else) should call: given an origin country
and a destination country, return a risk breakdown across every transport
mode that's actually viable for that lane, based on the latest computed
risk_scores for whichever tracked chokepoints that lane plausibly passes
through.

Deliberately knows nothing about suppliers, orders, quantities, or pricing —
it only answers "what does the risk landscape look like for shipping
between these two places, by each mode." That separation is intentional:
this stays reusable no matter how the procurement side changes.

This is directional, not a guarantee. It reflects publicly reported events
picked up in roughly the last 72 hours — not real-time ground truth, and
not confirmation that an unflagged route is actually safe. See the
`disclaimer` field returned with every response and surface it to users,
not just log it.

Change from v1: uses the shared pooled get_conn() instead of opening a
fresh psycopg2 connection per call.
"""

from typing import Optional

from .db import get_conn
from .route_inference import get_relevant_chokepoints, mode_viability

MODES = ["sea", "air", "road", "rail"]

# get_relevant_chokepoints expects chokepoints.mode values, which differ
# from the user-facing mode names ('sea' -> 'maritime', 'road' -> 'land').
MODE_TO_DB_MODE = {
    "sea": "maritime",
    "air": "air",
    "road": "land",
    "rail": "rail",
}

STATUS_RANK = {"green": 0, "yellow": 1, "red": 2}


def _latest_score_for_chokepoint(cur, chokepoint_id) -> Optional[dict]:
    cur.execute(
        """
        SELECT score, status, computed_at
        FROM risk_scores
        WHERE chokepoint_id = %s
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        (chokepoint_id,),
    )
    return cur.fetchone()


def _driving_events_for_chokepoints(cur, chokepoint_ids: list) -> list[dict]:
    if not chokepoint_ids:
        return []
    cur.execute(
        """
        SELECT e.headline, e.summary, e.severity, e.confidence, e.event_time,
               c.name AS chokepoint_name
        FROM events e
        JOIN event_chokepoints ec ON ec.event_id = e.id
        JOIN chokepoints c ON c.id = ec.chokepoint_id
        WHERE c.id = ANY(%s::uuid[])
        ORDER BY e.event_time DESC
        LIMIT 5
        """,
        (chokepoint_ids,),
    )
    return cur.fetchall()


def _summary_for(mode: str, status: str) -> str:
    labels = {"sea": "Sea freight", "air": "Air freight", "road": "Road freight", "rail": "Rail freight"}
    label = labels.get(mode, mode)
    return {
        "green": f"{label}: no significant disruption signals found on this lane's tracked chokepoints recently.",
        "yellow": f"{label}: elevated risk signals on part of this lane — worth monitoring, consider buffer time.",
        "red": f"{label}: active disruption signals on this lane's tracked chokepoints — expect likely delays or disruption.",
        "unknown": f"{label}: no tracked chokepoints identified on this lane, so risk data isn't available. This does not mean the route is risk-free.",
    }.get(status, f"{label}: risk data not currently available for this lane.")


def _assess_mode(cur, mode: str, origin_country: str, destination_country: str) -> dict:
    viable, reason = mode_viability(mode, origin_country, destination_country)
    if not viable:
        return {"mode": mode, "viable": False, "reason": reason}

    db_mode = MODE_TO_DB_MODE[mode]
    chokepoints = get_relevant_chokepoints(cur, db_mode, origin_country, destination_country)

    if not chokepoints:
        return {
            "mode": mode,
            "viable": True,
            "chokepoints": [],
            "status": "unknown",
            "score": None,
            "driving_events": [],
            "summary": _summary_for(mode, "unknown"),
        }

    enriched = []
    chokepoint_ids = []
    for cp in chokepoints:
        latest = _latest_score_for_chokepoint(cur, cp["id"])
        enriched.append({
            "name": cp["name"],
            "region": cp["region"],
            "score": float(latest["score"]) if latest else None,
            "status": latest["status"] if latest else "unknown",
            "computed_at": latest["computed_at"] if latest else None,
        })
        chokepoint_ids.append(cp["id"])

    statuses = [c["status"] for c in enriched if c["status"] in STATUS_RANK]
    overall_status = max(statuses, key=lambda s: STATUS_RANK[s]) if statuses else "unknown"
    scores = [c["score"] for c in enriched if c["score"] is not None]
    overall_score = max(scores) if scores else None

    driving_events = _driving_events_for_chokepoints(cur, chokepoint_ids)

    return {
        "mode": mode,
        "viable": True,
        "chokepoints": enriched,
        "status": overall_status,
        "score": overall_score,
        "driving_events": driving_events,
        "summary": _summary_for(mode, overall_status),
    }


def assess_route_risk(origin_country: str, destination_country: str) -> dict:
    """
    Main entry point. Given two country names, returns a risk breakdown
    across sea/air/road/rail, using the latest computed risk_scores for
    whichever tracked chokepoints are relevant to each viable mode on
    this lane.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            modes = [_assess_mode(cur, mode, origin_country, destination_country) for mode in MODES]

    return {
        "origin": origin_country,
        "destination": destination_country,
        "modes": modes,
        "disclaimer": (
            "Based on publicly reported events from roughly the last 72 hours. "
            "A route with no flagged risk means no negative signal was found — "
            "not a guarantee of safety."
        ),
    }


if __name__ == "__main__":
    import json
    result = assess_route_risk("Taiwan", "Netherlands")
    print(json.dumps(result, indent=2, default=str))