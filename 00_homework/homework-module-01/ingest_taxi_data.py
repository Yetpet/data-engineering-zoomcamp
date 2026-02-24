#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import requests


@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL username')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5433, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', type=int, default=2025, help='Year of data')
@click.option('--month', type=int, default=11, help='Month of data')
@click.option('--target-table', default='green_taxi_trips', help='Target table name')
@click.option('--chunksize', type=int, default=100000, help='Chunk size (used conceptually)')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):

    # -----------------------------
    # Build URL + filename
    # -----------------------------
    month_str = f"{month:02d}"
    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"green_tripdata_{year}-{month_str}.parquet"
    )
    local_file = f"green_tripdata_{year}-{month_str}.parquet"

    # -----------------------------
    # Download file (Python-native)
    # -----------------------------
    if not os.path.exists(local_file):
        r = requests.get(url)
        r.raise_for_status()
        with open(local_file, "wb") as f:
            f.write(r.content)

    print("Green taxi trip data downloaded locally")

    # -----------------------------
    # Database connection
    # -----------------------------
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # -----------------------------
    # Read parquet (single load)
    # -----------------------------
    df_green = pd.read_parquet(local_file)

    first = True
    total_rows = 0

    for df_chunk in tqdm(
        (df_green[i:i + chunksize] for i in range(0, len(df_green), chunksize))
    ):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace',
                index=False
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )

        total_rows += len(df_chunk)

    print(f"Green taxi trips data load complete - {total_rows} rows")

    # -----------------------------
    # Zones dataset
    # -----------------------------
    zones_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/"
        "releases/download/misc/taxi_zone_lookup.csv"
    )

    df_zones = pd.read_csv(zones_url)
    df_zones.to_sql(
        name='taxi_zones',
        con=engine,
        if_exists='replace',
        index=False
    )

    print(f"Zones data loaded - {len(df_zones)} rows")


if __name__ == '__main__':
    run()
