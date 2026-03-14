# Question 2
## Producer
[producer.ipynb](https://github.com/davidf552/data-engineering-zoomcamp/blob/main/Module07_Homework/producer.ipynb)


The important part of the code is this:

```python
import time

topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

```
# Question 3
## Consumer
[consumer.ipynb](https://github.com/davidf552/data-engineering-zoomcamp/blob/main/Module07_Homework/consumer.ipynb)

```sql
SELECT COUNT (1)
FROM green_trips
WHERE trip_distance > 5;
```

# Question 4
## Job
[pickup_job.py](https://github.com/davidf552/data-engineering-zoomcamp/blob/main/Module07_Homework/jobs/pickup_job.py)
```sql
SELECT PULocationID, num_trips
FROM green_trips_aggregated
ORDER BY num_trips DESC
LIMIT 3;
```
# Question 5
## Job
[long_job.py](https://github.com/davidf552/data-engineering-zoomcamp/blob/main/Module07_Homework/jobs/long_job.py)
```sql
SELECT PULocationID, MAX(num_trips) AS longest_session
FROM green_trips_session
GROUP BY PULocationID
ORDER BY longest_session DESC
LIMIT 1;
```

# Question 6
## Job
[hour_job.py](https://github.com/davidf552/data-engineering-zoomcamp/blob/main/Module07_Homework/jobs/hour_job.py)
```sql
SELECT window_start, total_tip
FROM green_trips_hourly_tips
ORDER BY total_tip DESC
LIMIT 1;


```
