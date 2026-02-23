/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion.trips
  - ingestion.payment_lookup

custom_checks:
  - name: row_count_greater_than_zero
    description: checks record counts from trips to be greater than 0
    value: 1
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

-- SELECT *
-- FROM ingestion.trips
-- WHERE pickup_datetime >= '{{ start_datetime }}'
--   AND pickup_datetime < '{{ end_datetime }}'

-- SELECT
--     t.pickup_datetime,
--     t.dropoff_datetime,
--     t.pickup_location_id,
--     t.dropoff_location_id,
--     t.fare_amount,
--     t.taxi_type,
--     p.payment_type_name
-- FROM ingestion.trips t
-- LEFT JOIN ingestion.payment_lookup p
--     ON t.payment_type = p.payment_type_id
-- WHERE t.pickup_datetime >= '{{ start_datetime }}'
--   AND t.pickup_datetime < '{{ end_datetime }}'
-- QUALIFY ROW_NUMBER() OVER (
--     PARTITION BY t.pickup_datetime, t.dropoff_datetime,
--                  t.pickup_location_id, t.dropoff_location_id, t.fare_amount
--     ORDER BY t.pickup_datetime
-- ) = 1

with ingested_trips as (
  select
    vendor_id,
    CAST(COALESCE(lpep_pickup_datetime,tpep_pickup_datetime) AS TIMESTAMP) AS pickup_datetime,
    CAST(COALESCE(lpep_dropoff_datetime,tpep_dropoff_datetime) AS TIMESTAMP) AS dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    -- case
    --   when taxi_type = 'yellow' then 0
    --   else ehail_fee
    -- end as ehail_fee,
    -- cbd_congestion_fee,
    taxi_type,
    extracted_at
  from ingestion.trips
  WHERE 1=1
    -- AND DATE_TRUNC('month', CAST(COALESCE(lpep_pickup_datetime, lpep_pickup_datetime) AS TIMESTAMP)) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
    AND COALESCE(lpep_pickup_datetime,tpep_pickup_datetime) IS NOT NULL
    AND COALESCE(lpep_dropoff_datetime,tpep_dropoff_datetime) IS NOT NULL
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND taxi_type IS NOT NULL
),
enriched_trips AS ( -- Enrich trips with location and payment information using LEFT JOINs
  SELECT
    ct.pickup_datetime,
    ct.dropoff_datetime,
    EXTRACT(EPOCH FROM (ct.dropoff_datetime - ct.pickup_datetime)) AS trip_duration_seconds,
    ct.pickup_location_id,
    ct.dropoff_location_id,
    ct.taxi_type,
    ct.trip_distance,
    ct.passenger_count,
    ct.fare_amount,
    ct.tip_amount,
    ct.total_amount,
    -- pl.borough AS pickup_borough,
    -- pl.zone AS pickup_zone,
    -- dl.borough AS dropoff_borough,
    -- dl.zone AS dropoff_zone,
    ct.payment_type,
    pmt.payment_type_name,
    ct.extracted_at,
    CURRENT_TIMESTAMP AS updated_at
  FROM ingested_trips AS ct
  -- LEFT JOIN ingestion.taxi_zone_lookup AS pl
    -- ON ct.pickup_location_id = pl.location_id
  -- LEFT JOIN raw.taxi_zone_lookup AS dl
    -- ON ct.dropoff_location_id = dl.location_id
  LEFT JOIN ingestion.payment_lookup AS pmt
    ON ct.payment_type = pmt.payment_type_id
  WHERE 1=1
    -- filter out zero durations (trip cannot end at the same time it starts or before it starts)
    AND EXTRACT(EPOCH FROM (ct.dropoff_datetime - ct.pickup_datetime)) > 0
    -- filter out outlier durations that are too long, 8 hours (28800 seconds)
    AND EXTRACT(EPOCH FROM (ct.dropoff_datetime - ct.pickup_datetime)) < 28800
    -- filter out negative total amounts
    AND ct.total_amount >= 0
    -- Only include trips that were actually charged
    AND pmt.payment_type_name IN ('flex_fare', 'credit_card', 'cash')
    -- filter out negative trip distances as they are data quality issues (trip distance cannot be negative)
    AND ct.trip_distance >= 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      ct.pickup_datetime,
      ct.dropoff_datetime,
      ct.pickup_location_id,
      ct.dropoff_location_id,
      ct.taxi_type,
      ct.trip_distance,
      ct.passenger_count,
      ct.fare_amount,
      ct.tip_amount,
      ct.total_amount,
      ct.payment_type
    ORDER BY ct.extracted_at DESC
  ) = 1
)

select 
  pickup_datetime,
  dropoff_datetime,
  trip_duration_seconds,
  pickup_location_id,
  dropoff_location_id,
  taxi_type,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount,
  payment_type,
  payment_type_name,
  extracted_at,
  updated_at  
from enriched_trips
