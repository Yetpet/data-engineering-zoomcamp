## DLT Workshop Homework Questions


### Question 1
What is the start date and end date of the dataset?

```sql
select
    min(cast(trip_pickup_date_time as date)) as start_date,
    max(cast(trip_pickup_date_time as date)) as end_date
from "taxi_data"
```

Result:
```
start_date  | end_date
------------+----------
2009-06-01  | 2009-06-30
```

### Question 2
What proportion of trips are paid with credit card?

```sql
select (
         COUNT(
                CASE WHEN payment_type =  'Credit' THEN 1 END) /
                COUNT(*)
        ) * 100 as credit_card_payment_ratio 
from "taxi_data";

```

Result:
```
--------------|--------
payment_type	%
--------------|--------
Credit	        26.66
--------------|--------

```

### Question 3
What is the total amount of money generated in tips?

```sql
select
SUM(tip_amt) as total_tip_amount
from "taxi_data"
```

Result:
```
total_tip_amount
------------------
6063.410000000009
```