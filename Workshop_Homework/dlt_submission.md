 
# Question 1
## What is the start date and end date of the dataset?
2009-06-01 to 2009-07-01
```sql
SELECT
  MIN(trip_pickup_date_time) as first_date,
  MAX(trip_pickup_date_time) as final_date
FROM "trips"

```

# Question 2
## What proportion of trips are paid with credit card?
26.66%
```sql
WITH trips_str AS (
    SELECT TRY_CAST(payment_type AS VARCHAR) AS payment_type_str
    FROM trips
)
SELECT
    COUNT(*) AS total_trips,
    SUM(
        CASE
            WHEN payment_type_str = '1' THEN 1
            WHEN LOWER(payment_type_str) IN ('credit card','credit','card','crd','cc') THEN 1
            ELSE 0
        END
    ) AS credit_card_trips,
    SUM(
        CASE
            WHEN payment_type_str = '1' THEN 1
            WHEN LOWER(payment_type_str) IN ('credit card','credit','card','crd','cc') THEN 1
            ELSE 0
        END
    ) * 100.0 / COUNT(*) AS credit_card_share 
FROM trips_str;

```


# Question 3
## What is the total amount of money generated in tips?
$10,063.41

```sql
SELECT SUM(CAST(tip_amt AS DECIMAL(18,2))) AS total_tips
FROM trips;

```
