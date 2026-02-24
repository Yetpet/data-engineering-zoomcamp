"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: vendorid
    type: DOUBLE
    description: A code indicating the TPEP provider that provided the record
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (yellow taxis only)
  - name: lpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (green taxis only)
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (yellow taxis only)
  - name: lpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (green taxis only)
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
  - name: payment_type
    type: DOUBLE
    description: A numeric code signifying how the passenger paid for the trip
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax that is automatically triggered based on the metered rate in use
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: DOUBLE
    description: $0.30 improvement surcharge assessed on hailed trips at the flag drop
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge for trips that start, end or pass through the Manhattan Central Business District
  - name: airport_fee
    type: DOUBLE
    description: Airport fee for trips that start or end at an -airport
  - name: ehail_fee
    type: DOUBLE
    description: E-hail fee for trips that were e-hail pickups (e.g. via Curb or Arro apps)

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import os
import json
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    # start_date = os.environ["BRUIN_START_DATE"]
    # end_date = os.environ["BRUIN_END_DATE"]

    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")

    print("START:", start_date)
    print("END:", end_date)

    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])
    # taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}"))

    print("TAXI TYPES:", taxi_types)

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    all_data = []

    current = start_date.replace(day=1)

    while current <= end_date:
        year = current.year
        month = f"{current.month:02d}"

        for taxi_type in taxi_types:
            taxi_type = taxi_type.lower()  # 🔥 force lowercase
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month}.parquet"
            print("Fetching:", url)
            
            response = requests.get(url)
            print("Status:", response.status_code)
            if response.status_code == 200:
                df = pd.read_parquet(BytesIO(response.content))
                df["taxi_type"] = taxi_type
                df["extracted_at"] = pd.Timestamp.utcnow()
                all_data.append(df)

        # move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    if not all_data:
        raise ValueError(
            "No parquet files downloaded. Check date range and taxi_types."
        )

    final_dataframe = pd.concat(all_data, ignore_index=True)

    # return final_dataframe
    return final_dataframe
