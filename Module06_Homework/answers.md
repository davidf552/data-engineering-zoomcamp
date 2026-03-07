# Question 3

```python
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

df.registerTempTable('trips_data')

spark.sql("""
SELECT 
    count(1) as total_trips
FROM 
    trips_data
WHERE
    MONTH(tpep_pickup_datetime) = 11 AND DAY(tpep_pickup_datetime) = 15;

""").show()
```

# Question 4

```python
spark.sql("""
SELECT 
    (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS longest_trip
FROM 
    trips_data
ORDER BY
    longest_trip DESC
LIMIT 1;
""").show()
```

# Question 6

```python
df_zones = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df_result = df.join(df_zones,df.PULocationID == df_zones.LocationID)

df_result.registerTempTable('joined_data')

spark.sql("""
SELECT 
    Zone,
    COUNT(1) AS total_trips
FROM 
    joined_data
GROUP BY
    Zone
ORDER BY
    total_trips
LIMIT 5
;
""").show()

```
