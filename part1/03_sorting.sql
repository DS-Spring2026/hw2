SET use_cached_result = false;

SELECT
  z.Borough,
  COUNT(*) AS march_trips,
  SUM(t.total_amount) AS march_revenue
FROM trips_randomized t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
WHERE t.pickup_month = 3
GROUP BY z.Borough
ORDER BY march_revenue DESC;

SELECT
  z.Borough,
  COUNT(*) AS march_trips,
  SUM(t.total_amount) AS march_revenue
FROM trips_clustered_by_month t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
WHERE t.pickup_month = 3
GROUP BY z.Borough
ORDER BY march_revenue DESC;
