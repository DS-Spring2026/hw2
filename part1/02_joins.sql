SET use_cached_result = false;

SELECT
  z.Zone,
  COUNT(*) AS num_trips,
  SUM(t.total_amount) AS total_revenue
FROM trips_large t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY total_revenue DESC;

SELECT /*+ BROADCAST(z) */
  z.Zone,
  COUNT(*) AS num_trips,
  SUM(t.total_amount) AS total_revenue
FROM trips_large t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY total_revenue DESC;
