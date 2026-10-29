"""
Route inference: given an origin and destination country, figure out which
known chokepoints are plausibly relevant, per transport mode.

Approach (a deliberate v1 simplification, not a real routing engine):

  - Sea: three steps, most trusted first.
      1. Curated lanes: rows in the `routes` / `route_chokepoints` tables
         (either direction). Add a lane there and it takes effect immediately.
      2. Region corridors: each country belongs to a sea region (see
         get_sea_region in country_data.py) and SEA_CORRIDORS lists the tracked
         chokepoints a ship normally passes between two regions
         (e.g. South Asia <-> Northern Europe goes via Bab-el-Mandeb and Suez).
      3. Otherwise NOTHING is returned, so the lane shows as "unknown".
         The old straight-line guess between country centroids is off by
         default, because on a sea lane it often cuts across land and picked
         chokepoints a ship never passes (India -> Germany matched Hormuz and
         the Black Sea). Set SEA_GEOSPATIAL_FALLBACK=true to bring it back.

  - Air: geospatial. Draw a straight line between origin/destination centroids
    and find which air chokepoints fall near it (within AIR_PROXIMITY_METERS).
    Aircraft do fly roughly direct, so this proxy is reasonable.

  - Rail / Road: NOT geospatial. These only exist on a handful of fixed,
    known corridors, so they use the small curated lookups below. Extend
    RAIL_CORRIDORS / ROAD_CORRIDORS as you identify more real corridors.

Chokepoint names below must match chokepoints.name in the database exactly.
"""

import os
from typing import Optional

from .country_data import get_centroid, get_sea_region, is_landlocked, normalize_country

# Max distance (meters) from the origin-destination line for a chokepoint to
# be considered "on the route". Used for air, and for sea only when the
# fallback below is switched on.
SEA_PROXIMITY_METERS = 1_500_000
AIR_PROXIMITY_METERS = 2_000_000

SEA_GEOSPATIAL_FALLBACK = os.environ.get("SEA_GEOSPATIAL_FALLBACK", "false").lower() == "true"

# Alternative spellings that should be treated as the same country.
COUNTRY_ALIASES = {
    "usa": "united states",
    "us": "united states",
    "united states of america": "united states",
    "uk": "united kingdom",
    "uae": "united arab emirates",
}


def canonical_country(name: str) -> str:
    key = normalize_country(name)
    return COUNTRY_ALIASES.get(key, key)


# Tracked chokepoints a ship normally passes between two sea regions. Keys are
# frozensets of the two region names (order doesn't matter). Regions are
# defined in country_data.SEA_REGIONS. Only lanes listed here get a sea
# assessment; any other pair comes back "unknown" instead of a guess.
_EUROPE_VIA_SUEZ = ["Bab-el-Mandeb Strait", "Suez Canal"]
_EAST_ASIA_TO_SUEZ = ["Taiwan Strait", "Strait of Malacca", "Bab-el-Mandeb Strait", "Suez Canal"]

SEA_CORRIDORS: dict[frozenset, list[str]] = {
    # South Asia (India, Pakistan, ...)
    frozenset({"south_asia", "europe_north"}): _EUROPE_VIA_SUEZ,
    frozenset({"south_asia", "europe_med"}): _EUROPE_VIA_SUEZ,
    frozenset({"south_asia", "black_sea"}): _EUROPE_VIA_SUEZ + ["Black Sea Corridor"],
    frozenset({"south_asia", "gulf"}): ["Strait of Hormuz"],
    frozenset({"south_asia", "east_asia"}): ["Strait of Malacca", "Taiwan Strait"],
    frozenset({"south_asia", "southeast_asia"}): ["Strait of Malacca"],
    # East Asia (China, Taiwan, South Korea, Japan, ...)
    frozenset({"east_asia", "europe_north"}): _EAST_ASIA_TO_SUEZ,
    frozenset({"east_asia", "europe_med"}): _EAST_ASIA_TO_SUEZ,
    frozenset({"east_asia", "black_sea"}): _EAST_ASIA_TO_SUEZ + ["Black Sea Corridor"],
    frozenset({"east_asia", "gulf"}): ["Taiwan Strait", "Strait of Malacca", "Strait of Hormuz"],
    # South-East Asia (Vietnam, Thailand, Malaysia, Singapore, ...)
    frozenset({"southeast_asia", "europe_north"}): ["Strait of Malacca"] + _EUROPE_VIA_SUEZ,
    frozenset({"southeast_asia", "europe_med"}): ["Strait of Malacca"] + _EUROPE_VIA_SUEZ,
    frozenset({"southeast_asia", "gulf"}): ["Strait of Malacca", "Strait of Hormuz"],
    # Persian Gulf (UAE, Qatar, Kuwait, Bahrain, Iraq, Iran)
    frozenset({"gulf", "europe_north"}): ["Strait of Hormuz"] + _EUROPE_VIA_SUEZ,
    frozenset({"gulf", "europe_med"}): ["Strait of Hormuz"] + _EUROPE_VIA_SUEZ,
    frozenset({"gulf", "black_sea"}): ["Strait of Hormuz"] + _EUROPE_VIA_SUEZ + ["Black Sea Corridor"],
    # Black Sea (Ukraine, Russia, Romania, ...) to the rest of Europe
    frozenset({"black_sea", "europe_north"}): ["Black Sea Corridor"],
    frozenset({"black_sea", "europe_med"}): ["Black Sea Corridor"],
}

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
}


def _pair_key(a: str, b: str) -> frozenset:
    return frozenset({canonical_country(a), canonical_country(b)})


def _chokepoints_by_name(cur, names: list[str], db_mode: Optional[str] = None) -> list[dict]:
    if not names:
        return []
    if db_mode:
        cur.execute(
            "SELECT id, name, mode, region FROM chokepoints WHERE name = ANY(%s) AND mode = %s",
            (names, db_mode),
        )
    else:
        cur.execute(
            "SELECT id, name, mode, region FROM chokepoints WHERE name = ANY(%s)",
            (names,),
        )
    rows = cur.fetchall()
    # Keep the order the corridor lists them in (roughly origin to destination).
    order = {n: i for i, n in enumerate(names)}
    return sorted(rows, key=lambda r: order.get(r["name"], 999))


def _curated_route_chokepoints(cur, db_mode: str, origin: str, destination: str) -> list[dict]:
    """Chokepoints from lanes you added to the routes / route_chokepoints tables (either direction)."""
    o = canonical_country(origin)
    d = canonical_country(destination)
    cur.execute(
        """
        SELECT DISTINCT c.id, c.name, c.mode, c.region
        FROM routes r
        JOIN route_chokepoints rc ON rc.route_id = r.id
        JOIN chokepoints c ON c.id = rc.chokepoint_id
        WHERE r.mode = %s
          AND (
                (lower(r.origin_region) = %s AND lower(r.destination_region) = %s)
             OR (lower(r.origin_region) = %s AND lower(r.destination_region) = %s)
          )
        """,
        (db_mode, o, d, d, o),
    )
    return cur.fetchall()


def _sea_corridor_names(origin: str, destination: str) -> list[str]:
    a = get_sea_region(origin)
    b = get_sea_region(destination)
    if not a or not b or a == b:
        return []
    return SEA_CORRIDORS.get(frozenset({a, b}), [])


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


def _geospatial(cur, db_mode: str, origin_country: str, destination_country: str, max_meters: int) -> list[dict]:
    origin_ll = get_centroid(origin_country)
    dest_ll = get_centroid(destination_country)
    if not origin_ll or not dest_ll:
        return []  # unmapped country: caller treats this as "no data"
    return _chokepoints_near_line(cur, db_mode, origin_ll, dest_ll, max_meters)


def get_relevant_chokepoints(cur, db_mode: str, origin_country: str, destination_country: str) -> list[dict]:
    """
    db_mode uses the values stored in chokepoints.mode: 'maritime', 'air',
    'land', 'rail', not the user-facing 'sea'/'road' names.
    """
    if db_mode == "maritime":
        curated = _curated_route_chokepoints(cur, db_mode, origin_country, destination_country)
        if curated:
            return curated

        by_corridor = _chokepoints_by_name(
            cur, _sea_corridor_names(origin_country, destination_country), db_mode
        )
        if by_corridor:
            return by_corridor

        if SEA_GEOSPATIAL_FALLBACK:
            return _geospatial(cur, db_mode, origin_country, destination_country, SEA_PROXIMITY_METERS)
        return []

    if db_mode == "air":
        return _geospatial(cur, db_mode, origin_country, destination_country, AIR_PROXIMITY_METERS)

    if db_mode == "rail":
        names = RAIL_CORRIDORS.get(_pair_key(origin_country, destination_country), [])
    elif db_mode == "land":
        names = ROAD_CORRIDORS.get(_pair_key(origin_country, destination_country), [])
    else:
        names = []

    return _chokepoints_by_name(cur, names)


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