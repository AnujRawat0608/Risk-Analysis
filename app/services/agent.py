"""
Q&A agent: answers natural-language questions like "Is the Suez route safe
for a container shipment this week?" or "What's the risk shipping from
Taiwan to the Netherlands?" using tool-calling over the live DB.

Rewritten for Groq's OpenAI-compatible chat completions API. Key difference
from the Anthropic version: Groq's tool-calling loop uses "tool" role
messages keyed by tool_call_id, rather than Anthropic's tool_use/tool_result
content blocks.

Change from v1: tool functions now use the shared pooled get_conn() instead
of opening a fresh psycopg2 connection per call -- a single question can
trigger 2-3 tool calls, each of which was previously its own connect/close.
"""

import os
import json
from groq import Groq

from .db import get_conn
from .route_risk_api import assess_route_risk

GROQ_MODEL = "llama-3.3-70b-versatile"

# Cap on tool-calling turns per question. Prevents a confused model from
# looping on tool calls indefinitely and never returning an answer.
MAX_TOOL_ITERATIONS = 5

client = Groq()

# Groq's tool schema follows OpenAI's function-calling format: each tool is
# wrapped in {"type": "function", "function": {...}}, and parameters use
# standard JSON Schema — structurally different from Anthropic's flatter
# {"name", "description", "input_schema"} shape.
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_chokepoint_status",
            "description": "Get the current risk score, status, and recent driving events for a named chokepoint (e.g. 'Strait of Hormuz', 'Suez Canal').",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Chokepoint name or partial match"}
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_all_chokepoint_statuses",
            "description": "List every tracked chokepoint with its current status (green/yellow/red), optionally filtered by transport mode.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["maritime", "air", "land", "rail"],
                        "description": "Optional filter by transport mode",
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_alternate_routes",
            "description": "Find alternate routes for a given route name if the primary is degraded.",
            "parameters": {
                "type": "object",
                "properties": {"route_name": {"type": "string"}},
                "required": ["route_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "assess_route_risk",
            "description": (
                "Given an origin country and a destination country, return a risk "
                "breakdown across sea, air, road, and rail freight for that lane — "
                "which modes are viable, which tracked chokepoints are relevant to "
                "each, and their current status. Use this for general 'what's the "
                "risk shipping from X to Y' questions, as opposed to a question "
                "about one specific named chokepoint."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "origin_country": {"type": "string", "description": "Origin country name"},
                    "destination_country": {"type": "string", "description": "Destination country name"},
                },
                "required": ["origin_country", "destination_country"],
            },
        },
    },
]


def tool_get_chokepoint_status(name: str) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.name, c.mode, c.region, rs.score, rs.status, rs.computed_at
                FROM chokepoints c
                LEFT JOIN LATERAL (
                    SELECT score, status, computed_at FROM risk_scores
                    WHERE chokepoint_id = c.id ORDER BY computed_at DESC LIMIT 1
                ) rs ON true
                WHERE c.name ILIKE %s LIMIT 1
                """,
                (f"%{name}%",),
            )
            row = cur.fetchone()
            if not row:
                return {"error": f"No chokepoint found matching '{name}'"}

            cur.execute(
                """
                SELECT e.headline, e.summary, e.severity, e.event_time
                FROM events e
                JOIN event_chokepoints ec ON ec.event_id = e.id
                JOIN chokepoints c ON c.id = ec.chokepoint_id
                WHERE c.name ILIKE %s
                ORDER BY e.event_time DESC LIMIT 5
                """,
                (f"%{name}%",),
            )
            row["recent_events"] = cur.fetchall()
            return row


def tool_list_all_chokepoint_statuses(mode: str = None) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT c.name, c.mode, rs.score, rs.status
                FROM chokepoints c
                LEFT JOIN LATERAL (
                    SELECT score, status FROM risk_scores
                    WHERE chokepoint_id = c.id ORDER BY computed_at DESC LIMIT 1
                ) rs ON true
            """
            params = ()
            if mode:
                query += " WHERE c.mode = %s"
                params = (mode,)
            cur.execute(query, params)
            return cur.fetchall()


def tool_find_alternate_routes(route_name: str) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT alt.name, alt.mode
                FROM routes primary_r
                JOIN routes alt ON alt.is_alternate_for = primary_r.id
                WHERE primary_r.name ILIKE %s
                """,
                (f"%{route_name}%",),
            )
            return cur.fetchall()


def tool_assess_route_risk(origin_country: str, destination_country: str) -> dict:
    return assess_route_risk(origin_country, destination_country)


TOOL_DISPATCH = {
    "get_chokepoint_status": lambda i: tool_get_chokepoint_status(**i),
    "list_all_chokepoint_statuses": lambda i: tool_list_all_chokepoint_statuses(**i),
    "find_alternate_routes": lambda i: tool_find_alternate_routes(**i),
    "assess_route_risk": lambda i: tool_assess_route_risk(**i),
}

SYSTEM_PROMPT = """You are a supply chain risk analyst assistant. You answer questions about \
current geopolitical and logistical risk to global trade routes using ONLY the data returned by \
your tools — never invent or assume a status you haven't looked up. Always cite which chokepoints \
you checked. Be direct about uncertainty: if data is stale or thin, say so. You are supporting a \
human decision-maker, not replacing their judgment — avoid absolute claims like 'guaranteed safe'; \
use 'currently low risk based on available data' instead.

If the person asks about a specific named chokepoint or route, use get_chokepoint_status or \
find_alternate_routes. If they ask a general "what's the risk shipping from X to Y" question \
without naming a specific chokepoint, use assess_route_risk instead — it covers all transport \
modes for that lane in one call."""


def _serialize(obj):
    return json.loads(json.dumps(obj, default=str))


def answer_question(question: str) -> str:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]

    for _ in range(MAX_TOOL_ITERATIONS):
        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            max_tokens=1024,
        )

        response_message = completion.choices[0].message

        if not response_message.tool_calls:
            return response_message.content or ""

        # Groq requires the assistant message (including its tool_calls) to
        # be appended before any tool results — same requirement as OpenAI.
        messages.append(response_message.model_dump(exclude_unset=True))

        for tool_call in response_message.tool_calls:
            fn_name = tool_call.function.name

            try:
                fn_args = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError:
                result = {"error": f"Malformed arguments for tool '{fn_name}'"}
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(result),
                })
                continue

            fn = TOOL_DISPATCH.get(fn_name)
            try:
                result = fn(fn_args) if fn else {"error": "unknown tool"}
            except Exception as e:
                # A DB hiccup or bad args from the model shouldn't 500 the
                # whole request — feed the error back so the model can
                # tell the user it couldn't retrieve that data.
                result = {"error": f"Tool '{fn_name}' failed: {e}"}

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(_serialize(result)),
                }
            )

    return (
        "I wasn't able to reach a clear answer after several lookups — "
        "the question may be broader than I can resolve in one go. "
        "Try asking about a specific chokepoint or a specific origin/destination pair."
    )


if __name__ == "__main__":
    print(answer_question("Is it safe to ship from Taiwan to the Netherlands right now?"))