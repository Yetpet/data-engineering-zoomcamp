## Module 2 Homework

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

![homework datasets](../../../02-workflow-orchestration/images/homework.png)

As a hint, Kestra makes that process really easy:
1. You can leverage the backfill functionality in the [scheduled flow](../../../02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.

### Quiz Questions & Answers

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?

```
 134.5 MiB
```


2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

- `green_tripdata_2020-04.csv`


3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
```
 ANS: 24,648,499

select count(*) as total_2020_yellow 
from kestra-sandbox-486103.zoomcamp.yellow_tripdata
WHERE
    tpep_pickup_datetime >= '2020-01-01 00:00:00'
    AND tpep_dropoff_datetime < '2021-12-31 23:59:59';

```

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?


```
ANS: 1,734,051

select count(*) as total_2020_green
from kestra-sandbox-486103.zoomcamp.green_tripdata
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
    AND lpep_pickup_datetime < '2021-01-01 00:00:00';
```


5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?

```
ANS: 1,925,152

select count(*) as total_202103 from kestra-sandbox-486103.zoomcamp.yellow_tripdata_2021_03
```

6) How would you configure the timezone to New York in a Schedule trigger?
```
Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
```
