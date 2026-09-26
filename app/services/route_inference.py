"""
Route inference: given an origin and destination country, figure out which
known chokepoints are plausibly relevant, per transport mode.

Approach (a deliberate v1 simplification, not a real routing engine):

  - Sea / Air: geospatial. Draw a straight line between origin/destination
    country centroids and find which chokepoints of that mode fall near it
    (within SEA_PROXIMITY_METERS / AIR_PROXIMITY_METERS). This won't
    reconstruct actual shipping lanes or flight paths — those curve around
    coastlines, airspace restrictions, etc. — but it's a reasonable proxy
    for "which of our tracked chokepoints does this lane plausibly pass
    near." Good enough for country-level input; revisit if you move to
    port/airport-level input later.

  - Rail / Road: NOT geospatial. These only exist on a handful of fixed,
    known corridors — you can't infer "there's a rail line here" from two
    points on a map the way you can reason about a sea lane. Uses a small
    curated lookup instead. Extend RAIL_CORRIDORS / ROAD_CORRIDORS as you
    identify more real corridors that matter to your suppliers.
"""

from typing import Optional

from .country_data import get_centroid, is_landlocked, normalize_country

# Max distance (meters) from the origin-destination line for a chokepoint to
# be considered "on the route." Loosely tuned starting points — revisit once
# you see real results across more country pairs.
SEA_PROXIMITY_METERS = 1_500_000
AIR_PROXIMITY_METERS = 2_000_000

# Curated fixed corridors for modes that can't be inferred geospatially.
# Keys are frozensets of the two country names (order doesn't matter),
# values are chokepoint names exactly as stored in chokepoints.name.
RAIL_CORRIDORS: dict[frozenset, list[str]] = {
    frozenset({"china", "germany"}): ["China-Europe Rail (Kazakhstan corridor)"],
    frozenset({"china", "poland"}): ["China-Europe Rail (Kazakhstan corridor)"],
    frozenset({"china", "netherlands"}): ["China-Europe Rail (Kazakhstan corridor)"],
}

ROAD_CORRIDORS: dict[frozenset, list[str]] = {
    frozenset({"united states", "mexico"}): ["US-Mexico Border (Laredo)"],
    frozenset({"us", "mexico"}): ["US-Mexico Border (Laredo)"],
}


def _pair_key(a: str, b: str) -> frozenset:
    return frozenset({normalize_country(a), normalize_country(b)})


def _chokepoints_near_line(
    cur,
    db_mode: str,
    origin_ll,
    dest_ll,
    max_meters: int,
) -> list[dict]:
    """Find chokepoints near the straight-line route between two countries."""

    o_lat, o_lon = origin_ll
    d_lat, d_lon = dest_ll

    cur.execute(
        """
        WITH route_line AS (
            SELECT ST_MakeLine(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            ) AS line
        )
        SELECT
            c.id,
            c.name,
            c.mode,
            c.region,
            ST_Distance(
                c.location,
                route_line.line::geography
            ) AS distance_meters
        FROM chokepoints c
        CROSS JOIN route_line
        WHERE c.mode = %s
          AND ST_DWithin(
              c.location,
              route_line.line::geography,
              %s
          )
        ORDER BY distance_meters ASC
        """,
        (
            o_lon,   # longitude FIRST
            o_lat,   # latitude SECOND
            d_lon,
            d_lat,
            db_mode,
            max_meters,
        ),
    )

    return cur.fetchall()

def get_relevant_chokepoints(cur, db_mode: str, origin_country: str, destination_country: str) -> list[dict]:
    """
    db_mode uses the values stored in chokepoints.mode: 'maritime', 'air',
    'land', 'rail' — not the user-facing 'sea'/'road' names.
    """
    if db_mode in ("maritime", "air"):
        origin_ll = get_centroid(origin_country)
        dest_ll = get_centroid(destination_country)
        if not origin_ll or not dest_ll:
            return []  # unmapped country — caller treats this as "no data"

        max_meters = SEA_PROXIMITY_METERS if db_mode == "maritime" else AIR_PROXIMITY_METERS
        return _chokepoints_near_line(cur, db_mode, origin_ll, dest_ll, max_meters)

    if db_mode == "rail":
        names = RAIL_CORRIDORS.get(_pair_key(origin_country, destination_country), [])
    elif db_mode == "land":
        names = ROAD_CORRIDORS.get(_pair_key(origin_country, destination_country), [])
    else:
        names = []

    if not names:
        return []

    cur.execute(
        "SELECT id, name, mode, region FROM chokepoints WHERE name = ANY(%s)",
        (names,),
    )
    return cur.fetchall()


def mode_viability(mode: str, origin_country: str, destination_country: str) -> tuple[bool, Optional[str]]:
    """mode here is the user-facing name: 'sea', 'air', 'road', 'rail'.
    Returns (viable, reason_if_not_viable)."""
    if mode == "air":
        return True, None  # virtually any two countries have air links

    if mode == "sea":
        if is_landlocked(origin_country) or is_landlocked(destination_country):
            return False, (
                "One or both countries are landlocked — sea freight would "
                "require an additional land/rail leg to a port, which isn't "
                "modeled yet."
            )
        return True, None

    if mode == "rail":
        if _pair_key(origin_country, destination_country) in RAIL_CORRIDORS:
            return True, None
        return False, "No known rail corridor mapped between these countries."

    if mode == "road":
        if _pair_key(origin_country, destination_country) in ROAD_CORRIDORS:
            return True, None
        return False, "No known direct land border/road corridor mapped between these countries."

    return False, "Unknown mode."