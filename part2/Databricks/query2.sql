SET use_cached_result = false;
WITH summary_statistics AS (
  SELECT
	PULocationID,
	DOLocationID,
    AVG(elapsed) - 3 * STDDEV(elapsed) AS min_elapsed,
    AVG(elapsed) + 3 * STDDEV(elapsed) AS max_elapsed
  FROM (
    SELECT
	  PULocationID,
	  DOLocationID,
      DATE_DIFF(minute, tpep_dropoff_datetime, tpep_pickup_datetime) AS elapsed
    FROM nyc_taxi_22_25
    WHERE DATE_DIFF(minute, tpep_dropoff_datetime, tpep_pickup_datetime) > 0
  )
  GROUP BY PULocationID, DOLocationID
)
SELECT *
FROM (
  SELECT
    NOT (
      DATE_DIFF(minute, tpep_dropoff_datetime, tpep_pickup_datetime)
      BETWEEN min_elapsed AND max_elapsed
    ) AS is_fraud,
    min_elapsed,
    max_elapsed,
    DATE_DIFF(minute, tpep_dropoff_datetime, tpep_pickup_datetime) AS elapsed,
    t1.*
  FROM nyc_taxi_22_25 AS t1
  LEFT JOIN summary_statistics AS t2
    USING (PULocationID, DOLocationID)
) foo
WHERE is_fraud;