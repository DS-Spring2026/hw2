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

CREATE OR REPLACE TEMP VIEW trips_with_month AS
SELECT
  *,
  month(tpep_pickup_datetime) AS pickup_month,
  date_trunc('MONTH', tpep_pickup_datetime) AS pickup_month_start
FROM nyc_taxi_22_25;

CREATE OR REPLACE TABLE trips_clustered_by_month
AS
SELECT *
FROM trips_with_month
ORDER BY pickup_month;

CREATE OR REPLACE TABLE trips_randomized
AS
SELECT *
FROM trips_with_month
ORDER BY rand();
