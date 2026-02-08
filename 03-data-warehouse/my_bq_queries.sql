CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.ext_yellow_trip_2024`
OPTIONS (
  format = 'parquet',
  uris = ['gs://demo-zoomcamp-mod3-dwh/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `zoomcamp.e_yellow_trip_2024`
AS SELECT * FROM `zoomcamp.ext_yellow_trip_2024`;

select count(*) from `zoomcamp.ext_yellow_trip_2024`;
select count(*) from `zoomcamp.e_yellow_trip_2024`;
select count(*) from `zoomcamp.e_yellow_trip_pc_2024`;


--Query Bytes 0B
select count(distinct PULocationID) from `zoomcamp.ext_yellow_trip_2024`;
--Query Bytes 155.12MB
select count(distinct PULocationID) from `zoomcamp.e_yellow_trip_2024`;

--Query Bytes 155.12MB
select PULocationID from `zoomcamp.e_yellow_trip_2024`;
--Query Bytes 310.24MB
select PULocationID,DOLocationID from `zoomcamp.e_yellow_trip_2024`;

--ANSWER - 8,333
select count(*) from `zoomcamp.e_yellow_trip_2024`
where fare_amount = 0;


CREATE OR REPLACE TABLE zoomcamp.e_yellow_trip_pc_2024
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp.e_yellow_trip_2024;

--materialized non-partitioned table - Query Bytes 310.24MB
select distinct VendorID from `zoomcamp.e_yellow_trip_2024`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'

--materialized partitioned table - Query Bytes 28.83MB
select distinct VendorID from `zoomcamp.e_yellow_trip_pc_2024`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'


