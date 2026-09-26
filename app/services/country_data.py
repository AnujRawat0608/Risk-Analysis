"""
Static country reference data used for route inference.

This is a pragmatic v1 approximation: real logistics routing depends on the
specific port/airport/city, not just the country. Using country centroids
here is a known simplification -- accurate enough to identify which major
chokepoints matter for a lane, not precise enough to plan an actual sailing
route. Swap this out for real port/airport-level geocoding later if you
want more precision; for now this is fine for country-level input.

Change from v1: for large, multi-coast countries, a geographic centroid can
sit nowhere near where that country's shipping actually happens (Russia's
centroid is in the middle of Siberia; a straight line from India to it
passes nowhere near a real port-to-port sea route, so no chokepoint ever
matches and the assessment silently comes back "unknown"). MAJOR_PORT_OVERRIDES
replaces the centroid with a real, representative trading port's coordinates
for a small set of countries where this matters. This is still a single
point standing in for a whole country -- a US->China shipment and a
US->Netherlands shipment would realistically leave from different US
coasts, and one override point can't capture that. It's a better
approximation than a geographic centroid, not a solved problem; the real
fix is destination-aware port selection or true port-level input.

Extend COUNTRY_CENTROIDS, MAJOR_PORT_OVERRIDES, and LANDLOCKED_COUNTRIES as
you onboard lanes that aren't covered yet -- an unmapped country just means
that mode's assessment comes back as "route data not available" rather than
crashing.
"""

from typing import Optional

# Approximate centroid (or a point reasonably representative of where trade
# actually originates/lands) for common trading nations. (lat, lon).
COUNTRY_CENTROIDS: dict[str, tuple[float, float]] = {
    "united states": (39.8, -98.6),
    "usa": (39.8, -98.6),
    "us": (39.8, -98.6),
    "china": (35.9, 104.2),
    "india": (21.1, 78.7),
    "taiwan": (23.7, 121.0),
    "south korea": (36.5, 127.8),
    "japan": (36.2, 138.3),
    "vietnam": (14.1, 108.3),
    "netherlands": (52.1, 5.3),
    "germany": (51.2, 10.5),
    "united kingdom": (54.0, -2.5),
    "uk": (54.0, -2.5),
    "france": (46.6, 2.2),
    "italy": (42.5, 12.6),
    "spain": (40.0, -3.7),
    "mexico": (23.6, -102.5),
    "canada": (56.1, -106.3),
    "brazil": (-10.3, -53.2),
    "argentina": (-38.4, -63.6),
    "australia": (-25.3, 133.8),
    "singapore": (1.35, 103.8),
    "malaysia": (4.2, 101.9),
    "indonesia": (-0.8, 113.9),
    "thailand": (15.9, 100.9),
    "united arab emirates": (23.4, 53.8),
    "uae": (23.4, 53.8),
    "saudi arabia": (23.9, 45.1),
    "egypt": (26.8, 30.8),
    "turkey": (38.9, 35.2),
    "south africa": (-30.6, 22.9),
    "nigeria": (9.1, 8.7),
    "russia": (61.5, 105.3),
    "poland": (51.9, 19.1),
    "kazakhstan": (48.0, 66.9),
    "ukraine": (48.4, 31.2),
    "israel": (31.0, 34.8),
    "pakistan": (30.4, 69.3),
    "bangladesh": (23.7, 90.4),
    "philippines": (12.9, 121.8),
    "chile": (-35.7, -71.5),
    "colombia": (4.6, -74.3),
    "switzerland": (46.8, 8.2),
    "sweden": (60.1, 18.6),
    "belgium": (50.5, 4.5),
}

# For large / multi-coast countries, the geographic centroid above can be
# far from any real port -- Russia's centroid is deep in Siberia, Canada's
# is in the middle of Hudson Bay's hinterland. These override the centroid
# with a single major, real trading port instead. This is still one point
# per country (see module docstring for the limitation that implies), but
# it's a materially better proxy than a geographic middle for these cases.
# Add a country here only when its plain centroid is actually misleading
# for sea-route inference -- most compact, single-coastline countries don't
# need one; their centroid is already close enough to their real ports.
MAJOR_PORT_OVERRIDES: dict[str, tuple[float, float]] = {
    "russia": (44.72, 37.78),      # Novorossiysk -- Russia's main Black Sea / Mediterranean-facing port
    "united states": (33.74, -118.27),  # Port of Los Angeles -- busiest US port, Asia-Pacific facing
    "usa": (33.74, -118.27),
    "us": (33.74, -118.27),
    "canada": (49.29, -123.11),    # Port of Vancouver -- Canada's largest, Asia-Pacific facing
    "brazil": (-23.96, -46.30),    # Port of Santos -- Brazil's largest port
    "australia": (-37.84, 144.93), # Port of Melbourne -- Australia's busiest container port
}

# Countries with no direct coastline. Not exhaustive -- extend as needed.
LANDLOCKED_COUNTRIES = {
    "switzerland", "austria", "kazakhstan", "mongolia", "nepal", "bolivia",
    "paraguay", "zambia", "zimbabwe", "uganda", "rwanda", "hungary",
    "slovakia", "czech republic", "afghanistan", "ethiopia", "chad", "mali",
    "niger", "belarus", "armenia", "azerbaijan", "luxembourg",
}


def normalize_country(name: str) -> str:
    return name.strip().lower()


def get_centroid(country: str) -> Optional[tuple[float, float]]:
    """
    Returns the major-port override when one exists for this country
    (see MAJOR_PORT_OVERRIDES docstring above), otherwise falls back to the
    plain geographic centroid.
    """
    key = normalize_country(country)
    if key in MAJOR_PORT_OVERRIDES:
        return MAJOR_PORT_OVERRIDES[key]
    return COUNTRY_CENTROIDS.get(key)


def is_landlocked(country: str) -> bool:
    return normalize_country(country) in LANDLOCKED_COUNTRIES