"""
NYC Taxi REST Source pipeline (DLT tutorial style).

- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Data is paginated (1,000 records per page). The resource will page until an empty page is returned.
- Pipeline name: `taxi_pipeline`

Run with:
    python taxi_pipeline.py

If `dlt` is not installed:
    pip install dlt
"""

import sys
import time
import requests

try:
    import dlt
except Exception:
    print("dlt library not found. Install with: pip install dlt", file=sys.stderr)
    raise

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.source
def taxi_source():
    """DLT source for the NYC taxi paginated REST API."""

    @dlt.resource(
        name="trips",
        write_disposition="append",
        columns={
            "rate_code": {"data_type": "text"},
            "mta_tax": {"data_type": "text"},
        },
    )
    def trips():
        page = 1
        while True:
            resp = requests.get(BASE_URL, params={"page": page}, timeout=60)
            resp.raise_for_status()
            payload = resp.json()

            # payload may be a list or an object containing the list under a key.
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                # common list keys fallback
                items = (
                    payload.get("data")
                    or payload.get("items")
                    or payload.get("results")
                    or payload.get("rows")
                    or payload.get("records")
                    or []
                )
            else:
                items = []

            # stop when an empty page is returned
            if not items:
                break

            for it in items:
                yield it

            page += 1
            # be polite to the API
            time.sleep(0.1)

    return trips


def main():
    pipeline = dlt.pipeline(pipeline_name="taxi_pipeline", destination="duckdb")
    load_info = pipeline.run(taxi_source())
    print("Load finished.")
    print("load_info:", load_info)


if __name__ == "__main__":
    main()
