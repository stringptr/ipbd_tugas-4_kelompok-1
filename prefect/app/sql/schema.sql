CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.earthquake_usgs_gov (
    id TEXT PRIMARY KEY,
    raw_json JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.earthquake_usgs_gov_parsed (
    id               BIGSERIAL PRIMARY KEY,
    time             TIMESTAMPTZ,
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    depth            DOUBLE PRECISION,
    mag              DOUBLE PRECISION,
    mag_type         TEXT,
    nst              INTEGER,
    gap              DOUBLE PRECISION,
    dmin             DOUBLE PRECISION,
    rms              DOUBLE PRECISION,
    net              TEXT,
    source_id        TEXT,
    updated          TIMESTAMPTZ,
    place            TEXT,
    event_type       TEXT,
    horizontal_error DOUBLE PRECISION,
    depth_error      DOUBLE PRECISION,
    mag_error        DOUBLE PRECISION,
    mag_nst          INTEGER,
    status           TEXT,
    location_source  TEXT,
    mag_source       TEXT,
    UNIQUE (time, source_id)
);
