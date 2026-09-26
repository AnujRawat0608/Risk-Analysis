"""
Supply Chain Risk Agent — FastAPI backend.

Endpoints:
  GET  /chokepoints                 -> list all chokepoints with current status
  GET  /chokepoints/{id}            -> detail + recent events + score history
  GET  /routes                      -> list routes with rolled-up route-level status
  GET  /routes/{id}/alternatives    -> suggested alternate routes if status is yellow/red
  POST /ask                         -> natural-language Q&A over current risk state
  POST /route-risk                  -> generic origin/destination risk assessment,
                                        across all viable transport modes

Change from v2: every handler now uses the shared pooled get_conn() from
app.services.db instead of opening/closing a fresh psycopg2 connection per
request. Fixes the "works after a restart, then stops responding" pattern
caused by connection churn/leaks under repeated use.
"""
from dotenv import load_dotenv
load_dotenv()

import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.services.db import get_conn
from app.services.agent import answer_question
from app.services.route_risk_api import assess_route_risk

STATUS_RANK = {"green": 0, "yellow": 1, "red": 2}


def worst_status(statuses: list[Optional[str]]) -> Optional[str]:
    ranked = [s for s in statuses if s in STATUS_RANK]
    if not ranked:
        return None
    return max(ranked, key=lambda s: STATUS_RANK[s])


app = FastAPI(title="Supply Chain Risk Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this for production
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/chokepoints")
def list_chokepoints(mode: Optional[str] = None):
    """List all chokepoints with their most recent risk score/status."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT c.id, c.name, c.mode, c.region,
                       ST_X(c.location::geometry) AS lon,
                       ST_Y(c.location::geometry) AS lat,
                       rs.score, rs.status, rs.computed_at
                FROM chokepoints c
                LEFT JOIN LATERAL (
                    SELECT score, status, computed_at
                    FROM risk_scores
                    WHERE chokepoint_id = c.id
                    ORDER BY computed_at DESC
                    LIMIT 1
                ) rs ON true
            """
            params = ()
            if mode:
                query += " WHERE c.mode = %s"
                params = (mode,)
            cur.execute(query, params)
            return cur.fetchall()


@app.get("/chokepoints/{chokepoint_id}")
def get_chokepoint(chokepoint_id: str):
    try:
        uuid.UUID(chokepoint_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid chokepoint id")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM chokepoints WHERE id = %s", (chokepoint_id,))
            cp = cur.fetchone()
            if not cp:
                raise HTTPException(status_code=404, detail="Chokepoint not found")

            cur.execute(
                """
                SELECT score, status, computed_at FROM risk_scores
                WHERE chokepoint_id = %s ORDER BY computed_at DESC LIMIT 50
                """,
                (chokepoint_id,),
            )
            cp["score_history"] = cur.fetchall()

            cur.execute(
                """
                SELECT e.id, e.headline, e.summary, e.event_type, e.severity,
                       e.confidence, e.event_time, e.source
                FROM events e
                JOIN event_chokepoints ec ON ec.event_id = e.id
                WHERE ec.chokepoint_id = %s
                ORDER BY e.event_time DESC LIMIT 20
                """,
                (chokepoint_id,),
            )
            cp["recent_events"] = cur.fetchall()
            return cp


@app.get("/routes")
def list_routes():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.id, r.name, r.mode, r.origin_region, r.destination_region,
                       rc.chokepoint_id, rs.status
                FROM routes r
                LEFT JOIN route_chokepoints rc ON rc.route_id = r.id
                LEFT JOIN LATERAL (
                    SELECT status FROM risk_scores
                    WHERE chokepoint_id = rc.chokepoint_id
                    ORDER BY computed_at DESC LIMIT 1
                ) rs ON true
                """
            )
            rows = cur.fetchall()

            routes: dict[str, dict] = {}
            for row in rows:
                r = routes.setdefault(row["id"], {
                    "id": row["id"], "name": row["name"], "mode": row["mode"],
                    "origin_region": row["origin_region"],
                    "destination_region": row["destination_region"],
                    "_statuses": [],
                })
                r["_statuses"].append(row["status"])

            result = []
            for r in routes.values():
                statuses = r.pop("_statuses")
                r["worst_status"] = worst_status(statuses)
                result.append(r)
            return result


@app.get("/routes/{route_id}/alternatives")
def get_alternatives(route_id: str):
    try:
        uuid.UUID(route_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid route id")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, mode, origin_region, destination_region
                FROM routes WHERE is_alternate_for = %s
                """,
                (route_id,),
            )
            return cur.fetchall()


class AskRequest(BaseModel):
    question: str


@app.post("/ask")
def ask(req: AskRequest):
    answer = answer_question(req.question)
    return {"question": req.question, "answer": answer}


class RouteRiskRequest(BaseModel):
    origin_country: str
    destination_country: str
    # Optional passthrough for the caller's own correlation/logging needs
    # (e.g. a request or order ID). Not used in the risk computation itself —
    # this endpoint stays generic and doesn't know what a "request" is.
    reference_id: Optional[str] = None


@app.post("/route-risk")
def route_risk(req: RouteRiskRequest):
    """
    Generic route risk lookup: given an origin and destination country,
    returns a risk breakdown across sea/air/road/rail freight — which modes
    are viable for this lane, the relevant tracked chokepoints for each,
    and their current status. Used by the RoutingPage frontend and by
    anything else (procurement or otherwise) that just needs "what's the
    risk shipping between these two places."
    """
    result = assess_route_risk(req.origin_country, req.destination_country)
    if req.reference_id:
        result["reference_id"] = req.reference_id
    return result


@app.get("/health")
def health():
    return {"status": "ok"}