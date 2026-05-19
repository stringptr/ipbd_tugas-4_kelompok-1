CREATE SCHEMA bronze;

CREATE TABLE IF NOT EXISTS bronze.earthquake_usgs_gov (
    id TEXT PRIMARY KEY,
    raw_json JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);
