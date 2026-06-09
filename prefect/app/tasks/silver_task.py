"""Tasks untuk pipeline silver layer — parsed earthquake data dari USGS."""

import datetime
import json
from typing import Optional

import requests
from prefect import task
from sqlalchemy import text

from utils.database import db_manager

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


@task(log_prints=True)
def get_last_parsed_time() -> Optional[datetime.datetime]:
    """
    Cek waktu event terakhir yang sudah ada di silver.earthquake_usgs_gov_parsed.

    Returns:
        Datetime dari event paling baru, atau None kalau table masih kosong.
    """
    with db_manager.get_connection() as conn:
        result = conn.execute(
            text("SELECT MAX(time) FROM silver.earthquake_usgs_gov_parsed")
        )
        last_time = result.scalar()

    if last_time is None:
        print("[INFO] silver table kosong, akan fetch dari awal (2015-01-01).")
    else:
        print(f"[INFO] Event terakhir di silver: {last_time.isoformat()}")

    return last_time


@task(log_prints=True, retries=3, retry_delay_seconds=10)
def fetch_and_insert_silver(starttime: str, endtime: str) -> int:
    """
    Fetch data gempa dari USGS untuk rentang waktu tertentu,
    parse field-nya, dan insert langsung ke silver.earthquake_usgs_gov_parsed.

    Args:
        starttime: ISO date string, misal "2025-01-01" atau "2025-01-15T12:30:00"
        endtime:   ISO date string batas atas (exclusive di USGS API)

    Returns:
        Jumlah baris yang berhasil diinsert.
    """
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime,
        "minlatitude": -11,
        "maxlatitude": 6,
        "minlongitude": 95,
        "maxlongitude": 141,
        "limit": 20000,
        "orderby": "time-asc",
    }

    print(f"[FETCH] Ambil data dari {starttime} s/d {endtime} ...")
    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    features = response.json().get("features", [])

    if not features:
        print(f"[INFO] Tidak ada data baru antara {starttime} dan {endtime}.")
        return 0

    rows = []
    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])

        time_ms = props.get("time")
        if time_ms is None or coords[0] is None:
            continue

        updated_ms = props.get("updated")

        rows.append({
            "time":             datetime.datetime.fromtimestamp(time_ms / 1000, tz=datetime.timezone.utc),
            "latitude":         coords[1],
            "longitude":        coords[0],
            "depth":            coords[2],
            "mag":              props.get("mag"),
            "mag_type":         props.get("magType"),
            "nst":              props.get("nst"),
            "gap":              props.get("gap"),
            "dmin":             props.get("dmin"),
            "rms":              props.get("rms"),
            "net":              props.get("net"),
            "source_id":        f.get("id"),
            "updated":          datetime.datetime.fromtimestamp(updated_ms / 1000, tz=datetime.timezone.utc) if updated_ms else None,
            "place":            props.get("place"),
            "event_type":       props.get("type"),
            "horizontal_error": props.get("horizontalError"),
            "depth_error":      props.get("depthError"),
            "mag_error":        props.get("magError"),
            "mag_nst":          props.get("magNst"),
            "status":           props.get("status"),
            "location_source":  props.get("locationSource"),
            "mag_source":       props.get("magSource"),
        })

    if not rows:
        print("[INFO] Semua feature dilewati (tidak ada time/coords).")
        return 0

    with db_manager.get_connection() as conn:
        conn.execute(
            text("""
                INSERT INTO silver.earthquake_usgs_gov_parsed (
                    time, latitude, longitude, depth,
                    mag, mag_type, nst, gap, dmin, rms,
                    net, source_id, updated, place, event_type,
                    horizontal_error, depth_error, mag_error, mag_nst,
                    status, location_source, mag_source
                ) VALUES (
                    :time, :latitude, :longitude, :depth,
                    :mag, :mag_type, :nst, :gap, :dmin, :rms,
                    :net, :source_id, :updated, :place, :event_type,
                    :horizontal_error, :depth_error, :mag_error, :mag_nst,
                    :status, :location_source, :mag_source
                )
                ON CONFLICT (time, source_id) DO NOTHING
            """),
            rows,
        )

    print(f"[OK] {len(rows)} baris diinsert ke silver ({starttime} s/d {endtime})")
    return len(rows)
