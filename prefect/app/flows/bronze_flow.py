"""Flow pipeline data gempa bumi dari USGS ke bronze layer."""

from prefect import flow

from tasks.bronze_task import check_year_exists, extract_to_bronze, run_schema


@flow(name="bronze-earthquake-pipeline", log_prints=True)
def pipeline(start_year: int = 2015, end_year: int = 2025):
    """
    Pipeline batch ingesti data gempa bumi dari USGS.

    Mengambil data gempa wilayah Indonesia (lat -11~6, lon 95~141)
    per bulan dari start_year hingga end_year, lalu menyimpannya
    ke bronze.earthquake_usgs_gov di PostgreSQL.

    Tahun yang datanya sudah ada di database akan di-skip otomatis.

    Args:
        start_year: Tahun awal ingesti (default 2015)
        end_year: Tahun akhir ingesti (default 2025)
    """
    # 1. Pastikan schema & tabel sudah ada
    run_schema()

    # 2. Ingest per tahun, skip jika sudah ada
    total_inserted = 0
    for year in range(start_year, end_year + 1):
        already_exists = check_year_exists(year)
        if already_exists:
            continue

        for month in range(1, 13):
            count = extract_to_bronze(year, month)
            total_inserted += count

    print(f"[DONE] Total baris diinsert: {total_inserted}")


if __name__ == "__main__":
    pipeline()
