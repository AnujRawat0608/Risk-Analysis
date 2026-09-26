-- Supply Chain Risk Agent — Database Schema
-- Requires PostgreSQL with the PostGIS extension enabled.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- CHOKEPOINTS: the fixed set of critical transit nodes/corridors
-- ============================================================
CREATE TABLE chokepoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    mode            TEXT NOT NULL CHECK (mode IN ('maritime', 'air', 'land', 'rail')),
    region          TEXT,
    location        GEOGRAPHY(POINT, 4326) NOT NULL,
    boundary        GEOGRAPHY(POLYGON, 4326),
    daily_volume_usd NUMERIC,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- ROUTES: named trade corridors composed of an ordered sequence of chokepoints
-- ============================================================
CREATE TABLE routes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    mode            TEXT NOT NULL CHECK (mode IN ('maritime', 'air', 'land', 'rail', 'multimodal')),
    origin_region   TEXT,
    destination_region TEXT,
    is_alternate_for UUID REFERENCES routes(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE route_chokepoints (
    route_id        UUID NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    chokepoint_id   UUID NOT NULL REFERENCES chokepoints(id) ON DELETE CASCADE,
    sequence_order  INT NOT NULL,
    PRIMARY KEY (route_id, chokepoint_id, sequence_order)
);

-- ============================================================
-- EVENTS: individual risk signals extracted from news/GDELT/ACLED/etc.
-- ============================================================
CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT NOT NULL,
    source_ref      TEXT,
    event_type      TEXT NOT NULL CHECK (event_type IN (
                        'conflict', 'blockade', 'sanctions', 'strike',
                        'piracy', 'weather', 'congestion', 'regulatory', 'other'
                    )),
    headline        TEXT NOT NULL,
    summary         TEXT,
    severity        SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 5),
    confidence      NUMERIC(3,2) CHECK (confidence BETWEEN 0 AND 1),
    event_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    location        GEOGRAPHY(POINT, 4326),
    raw_payload     JSONB
);

CREATE TABLE event_chokepoints (
    event_id        UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    chokepoint_id   UUID NOT NULL REFERENCES chokepoints(id) ON DELETE CASCADE,
    impact_note     TEXT,
    PRIMARY KEY (event_id, chokepoint_id)
);

-- ============================================================
-- RISK SCORES: computed/rolled-up status per chokepoint over time
-- ============================================================
CREATE TABLE risk_scores (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chokepoint_id   UUID NOT NULL REFERENCES chokepoints(id) ON DELETE CASCADE,
    score           NUMERIC(4,2) NOT NULL CHECK (score BETWEEN 0 AND 100),
    status          TEXT NOT NULL CHECK (status IN ('green', 'yellow', 'red')),
    driving_event_ids UUID[],
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_risk_scores_chokepoint_time ON risk_scores (chokepoint_id, computed_at DESC);
CREATE INDEX idx_events_time ON events (event_time DESC);
CREATE INDEX idx_events_location ON events USING GIST (location);
CREATE INDEX idx_chokepoints_location ON chokepoints USING GIST (location);

-- Seed data: major global chokepoints across all modes
INSERT INTO chokepoints (name, mode, region, location, daily_volume_usd, description) VALUES
('Strait of Hormuz', 'maritime', 'Middle East', ST_GeogFromText('POINT(56.25 26.57)'), 1000000000, 'Chokepoint for Persian Gulf oil exports'),
('Suez Canal', 'maritime', 'Middle East/North Africa', ST_GeogFromText('POINT(32.34 30.5)'), 900000000, 'Links Mediterranean to Red Sea, key Asia-Europe route'),
('Strait of Malacca', 'maritime', 'Southeast Asia', ST_GeogFromText('POINT(100.35 2.5)'), 800000000, 'Busiest strait globally, links Indian Ocean to Pacific'),
('Bab-el-Mandeb Strait', 'maritime', 'Middle East/Horn of Africa', ST_GeogFromText('POINT(43.3 12.6)'), 700000000, 'Southern entrance to Red Sea/Suez route'),
('Panama Canal', 'maritime', 'Central America', ST_GeogFromText('POINT(-79.6 9.08)'), 270000000, 'Links Atlantic and Pacific, subject to drought-driven draft restrictions'),
('Taiwan Strait', 'maritime', 'East Asia', ST_GeogFromText('POINT(119.5 24.5)'), 600000000, 'Critical East Asia shipping and semiconductor logistics corridor'),
('Black Sea Corridor', 'maritime', 'Eastern Europe', ST_GeogFromText('POINT(31.0 45.3)'), 100000000, 'Grain/commodity export corridor, disrupted by Russia-Ukraine war'),
('Russian Airspace Corridor', 'air', 'Eurasia', ST_GeogFromText('POINT(60.0 60.0)'), NULL, 'Overflight corridor avoided by many carriers since 2022'),
('China-Europe Rail (Kazakhstan corridor)', 'rail', 'Central Asia', ST_GeogFromText('POINT(68.0 48.0)'), NULL, 'Overland rail freight route between China and Europe'),
('US-Mexico Border (Laredo)', 'land', 'North America', ST_GeogFromText('POINT(-99.5 27.5)'), 250000000, 'Busiest US-Mexico land freight crossing');