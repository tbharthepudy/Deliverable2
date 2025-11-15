import os
import json
import time

import requests
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv


load_dotenv()

SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
YEARS_CONFIG = os.getenv("YEARS_CONFIG", "years.json")


def load_years_config(path: str) -> dict:
    """Load {year: uuid} mapping from the local JSON file."""
    with open(path, "r") as f:
        return json.load(f)


def get_storage_client():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_KEY_PATH
    )
    client = storage.Client(credentials=creds, project=PROJECT_ID)
    return client


def fetch_year_rows(year: str, uuid: str) -> list[dict]:
    """
    Pull full year dataset (with pagination) and return a list of rows.
    """
    base_url = f"https://data.cms.gov/data-api/v1/dataset/{uuid}/data"
    limit = 5000
    offset = 0

    all_rows_for_year: list[dict] = []

    while True:
        url = f"{base_url}?size={limit}&offset={offset}"
        print(f"[{year}] Fetching offset={offset} ...")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()

        data = resp.json()
        print(f"[{year}] Retrieved {len(data)} rows in this page")

        if len(data) == 0:
            print(f"[{year}] Completed fetching all pages.")
            break

        # OPTIONAL: inject the year into each row for easier partitioning later
        for row in data:
            # only add if not already present
            # if "year" not in row:
            #     row["year"] = int(year)
            all_rows_for_year.append(row)

        offset += limit
        time.sleep(0.2)

    print(f"[{year}] Total rows fetched: {len(all_rows_for_year)}")
    return all_rows_for_year


def main():
    if not SERVICE_ACCOUNT_KEY_PATH or not BUCKET_NAME:
        raise ValueError(
            "Missing environment vars. Ensure .env contains GOOGLE_APPLICATION_CREDENTIALS and GCP_BUCKET_NAME."
        )

    # 1) load {year: uuid}
    years_map = load_years_config(YEARS_CONFIG)

    # 2) auth + bucket
    client = get_storage_client()
    bucket = client.bucket(BUCKET_NAME)

    # 3) fetch ALL years into one combined list
    all_rows: list[dict] = []
    for year, uuid in years_map.items():
        print(f"\n=== Starting ingest for year {year} ===")
        year_rows = fetch_year_rows(year, uuid)
        all_rows.extend(year_rows)

    print(f"\n*** Total rows across ALL years: {len(all_rows)} ***")

    # 4) upload ONE combined JSONL file
    jsonl_content = "\n".join(json.dumps(row) for row in all_rows)

    blob_name = "cms/raw/data.jsonl"  # single combined file path
    blob = bucket.blob(blob_name)
    blob.upload_from_string(jsonl_content, content_type="application/json")

    print(f"\n✅ Uploaded combined file: gs://{BUCKET_NAME}/{blob_name}")

if __name__ == "__main__":
    main()
