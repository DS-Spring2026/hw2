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
      TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, minute) AS elapsed
    FROM data342.taxi
    WHERE TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, minute) > 0
  )
  GROUP BY PULocationID, DOLocationID
)
SELECT *
FROM (
  SELECT
    NOT (
      TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, minute)
      BETWEEN min_elapsed AND max_elapsed
    ) AS is_fraud,
    min_elapsed,
    max_elapsed,
    DATE_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, minute) AS elapsed,
    t1.*
  FROM data342.taxi AS t1
  LEFT JOIN summary_statistics AS t2
    USING (PULocationID, DOLocationID)
) foo
WHERE is_fraud
LIMIT 10;
