SET use_cached_result = false;

CREATE OR REPLACE TABLE march_skewed AS
SELECT /*+ REPARTITION(4, worker_bucket) */
  *
FROM (
  SELECT
    *,
    month(tpep_pickup_datetime) AS pickup_month,
    CASE
      WHEN month(tpep_pickup_datetime) = 3 THEN 0
      ELSE 1 + pmod(hash(PULocationID), 3)
    END AS worker_bucket
  FROM workspace.default.nyc_taxi_22_25
) t;


CREATE OR REPLACE TABLE march_balanced AS
SELECT /*+ REPARTITION(4, worker_bucket) */
  *
FROM (
  SELECT
    *,
    month(tpep_pickup_datetime) AS pickup_month,
    pmod(
      hash(
        concat_ws(
          ':',
          cast(PULocationID as string),
          cast(DOLocationID as string),
          cast(VendorID as string),
          cast(fare_amount as string)
        )
      ),
      4
    ) AS worker_bucket
  FROM workspace.default.nyc_taxi_22_25
) t;

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