CREATE TABLE IF NOT EXISTS bronze_earthquakes (
    id TEXT PRIMARY KEY,
    raw_json JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);