"""Silver layer pipeline — batch incremental dari USGS ke parsed table."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime

from prefect import flow

from tasks.bronze_task import run_schema
from tasks.silver_task import get_last_parsed_time, fetch_and_insert_silver

# Batas awal kalau silver table masih kosong
DEFAULT_START = datetime.datetime(2015, 1, 1, tzinfo=datetime.timezone.utc)

# Ukuran chunk per fetch (hari) — USGS limit 20000 rows per request,
# chunk 30 hari aman untuk data Indonesia
CHUNK_DAYS = 30


@flow(name="silver-earthquake-pipeline", log_prints=True)
def pipeline():
    # Pastikan schema & table sudah ada
    run_schema()

    # Tentukan starttime berdasarkan data terakhir di silver
    last_time = get_last_parsed_time()
    if last_time is None:
        start_dt = DEFAULT_START
    else:
        # Mulai 1 detik setelah event terakhir supaya tidak dobel
        start_dt = last_time + datetime.timedelta(seconds=1)

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    if start_dt >= now:
        print("[DONE] Silver sudah up-to-date, tidak ada data baru untuk di-fetch.")
        return

    print(f"[INFO] Fetch dari {start_dt.isoformat()} s/d {now.isoformat()}")

    # Pecah jadi chunk supaya tidak kena limit USGS
    total_inserted = 0
    chunk_start = start_dt

    while chunk_start < now:
        chunk_end = min(chunk_start + datetime.timedelta(days=CHUNK_DAYS), now)

        count = fetch_and_insert_silver(
            starttime=chunk_start.strftime("%Y-%m-%dT%H:%M:%S"),
            endtime=chunk_end.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        total_inserted += count
        chunk_start = chunk_end

    print(f"[DONE] Total baris diinsert ke silver: {total_inserted}")


if __name__ == "__main__":
    pipeline()
