CREATE OR REPLACE TABLE trips_small AS
SELECT *
FROM nyc_taxi_22_25
LIMIT 10000;

CREATE OR REPLACE TABLE trips_medium AS
SELECT *
FROM nyc_taxi_22_25
LIMIT 100000;

CREATE OR REPLACE TABLE trips_large AS
SELECT *
FROM nyc_taxi_22_25
LIMIT 1000000;

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
