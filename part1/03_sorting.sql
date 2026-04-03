SET use_cached_result = false;

SELECT
  PULocationID,
  DOLocationID,
  total_amount,
  fare_amount,
  trip_distance
FROM march_skewed
WHERE pickup_month = 3
SORT BY total_amount DESC;

SELECT
  PULocationID,
  DOLocationID,
  total_amount,
  fare_amount,
  trip_distance
FROM march_balanced
WHERE pickup_month = 3
SORT BY total_amount DESC;
