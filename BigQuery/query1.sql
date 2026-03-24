WITH top_hh24 AS (
  SELECT hh24,
         AVG(n) AS num_avg_trip
  FROM (
    SELECT EXTRACT(day FROM tpep_pickup_datetime) AS day,
           EXTRACT(hour FROM tpep_dropoff_datetime) AS hh24,
           COUNT(1) AS n
    FROM data342.taxi
    GROUP BY 1, 2
  ) foo
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 3
),
top_vendor AS (
  SELECT VendorID,
         AVG(n_pass)
  FROM (
    SELECT EXTRACT(day FROM tpep_pickup_datetime) AS day,
           VendorID,
           SUM(COALESCE(passenger_count, 0)) AS n_pass
    FROM data342.taxi t1
    JOIN top_hh24 t2
      ON (EXTRACT(hour FROM t1.tpep_pickup_datetime) = t2.hh24)
    GROUP BY 1, 2
  ) foor
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 3
)
SELECT VendorID,
       SUM(total_amount) AS total_revenue
FROM data342.taxi
JOIN top_vendor USING (VendorID)
GROUP BY 1;
