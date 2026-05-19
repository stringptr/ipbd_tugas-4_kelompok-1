import requests
import pandas as pd
from prefect import flow, task
from sqlalchemy import text
import json

from utils.database import db_manager

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


# =========================
# INIT SCHEMA (WAJIB)
# =========================
@task(log_prints=True)
def run_schema():
    with open("sql/schema.sql", "r") as f:
        sql = f.read()

    with db_manager.get_connection() as conn:
        conn.execute(text(sql))

    print("Schema initialized")


# =========================
# EXTRACT + LOAD BRONZE
# =========================
@task(log_prints=True)
def extract_to_bronze(year: int, month: int):

    start_time = f"{year}-{month:02d}-01"
    end_time = f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"

    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minlatitude": -11,
        "maxlatitude": 6,
        "minlongitude": 95,
        "maxlongitude": 141,
        "limit": 20000,
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    rows = []

    for f in data.get("features", []):
        props = f.get("properties", {})
        geom = f.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])

        # skip invalid
        if not props.get("time") or coords[0] is None:
            continue

        rows.append(
            {
                "id": f.get("id"),
                "raw_json": {"id": f.get("id"), "properties": props, "geometry": geom},
            }
        )

    if not rows:
        print(f"No data for {year}-{month:02d}")
        return 0

    # =========================
    # BULK INSERT (FIXED JSONB)
    # =========================
    with db_manager.get_connection() as conn:
        conn.execute(
            text("""
                INSERT INTO bronze.earthquake_usgs_gov(id, raw_json)
                VALUES (:id, CAST(:raw_json AS JSONB))
                ON CONFLICT (id) DO NOTHING
            """),
            [{"id": r["id"], "raw_json": json.dumps(r["raw_json"])} for r in rows],
        )

    print(f"Bronze inserted: {len(rows)} rows")
    return len(rows)


# =========================
# FLOW
# =========================
@flow(name="earthquake-bronze-pipeline")
def pipeline(start_year=2015, end_year=2025):

    # 1. INIT TABLE DULU (FIX ERROR KAMU)
    run_schema()

    # 2. INGEST DATA
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            extract_to_bronze(year, month)


if __name__ == "__main__":
    pipeline()

