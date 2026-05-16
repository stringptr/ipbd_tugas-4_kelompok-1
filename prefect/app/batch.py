import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from prefect import flow, task

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
OUTPUT_DIR = Path("/prefect/data/earthquakes")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@task(log_prints=True)
def download_earthquake_data(year: int):
    print(f"Downloading earthquake data for {year}")

    start_time = f"{year}-01-01"
    end_time = f"{year}-12-31"

    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "eventtype": "earthquake",
        "minmagnitude": 0,
        "limit": 20000
    }
    response = requests.get(BASE_URL, params=params)
    print(response.status_code)
    print(response.text[:500])

    if response.status_code != 200:
        raise Exception(f"Failed request for year {year}")

    data = response.json()

    features = data.get("features", [])

    rows = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        coordinates = geom.get("coordinates", [None, None, None])

        rows.append({
            "id": feature.get("id"),
            "time": props.get("time"),
            "place": props.get("place"),
            "magnitude": props.get("mag"),
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth": coordinates[2],
            "status": props.get("status"),
            "type": props.get("type"),
            "url": props.get("url")
        })

    df = pd.DataFrame(rows)

    output_file = OUTPUT_DIR / f"earthquake_{year}.csv"

    df.to_csv(output_file, index=False)

    print(f"Saved {len(df)} rows to {output_file}")

    return str(output_file)


@flow(name="earthquake-download-flow")
def earthquake_download_flow(start_year: int = 2015, end_year: int = 2025):

    downloaded_files = []

    for year in range(start_year, end_year + 1):
        file_path = download_earthquake_data(year)
        downloaded_files.append(file_path)

    print("All downloads completed")
    return downloaded_files

if __name__ == "__main__":
    earthquake_download_flow()