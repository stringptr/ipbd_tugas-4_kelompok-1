from prefect import flow

from tasks.bronze_task import check_year_exists, extract_to_bronze, run_schema


@flow(name="bronze-earthquake-pipeline", log_prints=True)
def pipeline(start_year: int = 2015, end_year: int = 2025):
    run_schema()
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
