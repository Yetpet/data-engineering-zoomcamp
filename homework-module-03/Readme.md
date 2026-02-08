# Module 3 Homework: Data Warehousing & BigQuery

In this homework we'll practice working with BigQuery and Google Cloud Storage.


## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 

```
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.ext_yellow_trip_2024`
OPTIONS (
  format = 'parquet',
  uris = ['gs://demo-zoomcamp-mod3-dwh/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE `zoomcamp.e_yellow_trip_2024`
AS SELECT * FROM `zoomcamp.ext_yellow_trip_2024`;
```

## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?
```
--ANSWER: 20,332,093
select count(*) from `zoomcamp.e_yellow_trip_2024`;
```

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 155.12 MB for the Materialized Table

```
--Query Bytes 0B
select count(distinct PULocationID) from `zoomcamp.ext_yellow_trip_2024`;

--Query Bytes 155.12MB
select count(distinct PULocationID) from `zoomcamp.e_yellow_trip_2024`;
```
## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

```
--Query Bytes 155.12MB
select PULocationID from `zoomcamp.e_yellow_trip_2024`;

--Query Bytes 310.24MB
select PULocationID,DOLocationID from `zoomcamp.e_yellow_trip_2024`;
```

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?
- 8,333

```
-- ANSWER - 8,333

select count(*) from `zoomcamp.e_yellow_trip_2024`
where fare_amount = 0;
```
## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- Partition by tpep_dropoff_datetime and Cluster on VendorID

```
CREATE OR REPLACE TABLE zoomcamp.e_yellow_trip_pc_2024
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp.e_yellow_trip_2024;
```

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)


Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 


Choose the answer which most closely matches.
 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table


## Question 7. External table storage

Where is the data stored in the External Table you created?

- GCP Bucket


## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:
- False


## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

- It estimates that 0B will be read because BigQuery is a columnar database and the `SELECT count(*)` query does not require reading any columns, it only needs to read the metadata of the table to get the count of the number of rows.

```
-- ANSWER: 0B
select count(*) from `zoomcamp.e_yellow_trip_2024`;
```