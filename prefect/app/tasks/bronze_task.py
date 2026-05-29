"""Tasks untuk pipeline data gempa bumi dari USGS."""

import datetime
import json

import requests
from prefect import task
from sqlalchemy import text

from utils.database import db_manager

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


@task(log_prints=True)
def run_schema():
    """Inisialisasi schema dan tabel di database (idempotent)."""
    with open("sql/schema.sql", "r") as f:
        sql = f.read()

    with db_manager.get_connection() as conn:
        conn.execute(text(sql))

    print("Schema initialized")


@task(log_prints=True)
def check_year_exists(year: int) -> bool:
    """
    Cek apakah data untuk tahun tertentu sudah ada di bronze.

    Returns:
        True jika data tahun tersebut sudah ada, False jika belum.
    """
    start_ms = int(datetime.datetime(year, 1, 1).timestamp() * 1000)
    end_ms = int(datetime.datetime(year, 12, 31, 23, 59, 59).timestamp() * 1000)

    with db_manager.get_connection() as conn:
        result = conn.execute(
            text("""
                SELECT COUNT(*) FROM bronze.earthquake_usgs_gov
                WHERE (raw_json->'properties'->>'time')::bigint
                    BETWEEN :start_ms AND :end_ms
            """),
            {"start_ms": start_ms, "end_ms": end_ms},
        )
        count = result.scalar()

    exists = count > 0
    if exists:
        print(f"[SKIP] Tahun {year} sudah ada ({count} baris), skip fetch.")
    else:
        print(f"[FETCH] Tahun {year} belum ada, akan di-fetch.")

    return exists


@task(log_prints=True)
def extract_to_bronze(year: int, month: int) -> int:
    """
    Fetch data gempa dari USGS untuk satu bulan dan simpan ke bronze layer.

    Args:
        year: Tahun target
        month: Bulan target (1-12)

    Returns:
        Jumlah baris yang berhasil diinsert.
    """
    start_time = f"{year}-{month:02d}-01"
    end_time = (
        f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"
    )

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
    response.raise_for_status()
    data = response.json()

    rows = []
    for f in data.get("features", []):
        props = f.get("properties", {})
        geom = f.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])

        if not props.get("time") or coords[0] is None:
            continue

        rows.append(
            {
                "id": f.get("id"),
                "raw_json": {"id": f.get("id"), "properties": props, "geometry": geom},
            }
        )

    if not rows:
        print(f"[INFO] Tidak ada data untuk {year}-{month:02d}")
        return 0

    with db_manager.get_connection() as conn:
        conn.execute(
            text("""
                INSERT INTO bronze.earthquake_usgs_gov(id, raw_json)
                VALUES (:id, CAST(:raw_json AS JSONB))
                ON CONFLICT (id) DO NOTHING
            """),
            [{"id": r["id"], "raw_json": json.dumps(r["raw_json"])} for r in rows],
        )

    print(f"[OK] {year}-{month:02d}: {len(rows)} baris diinsert ke bronze")
    return len(rows)
