SET use_cached_result = false;

SELECT
  z.Borough,
  COUNT(*) AS num_trips,
  AVG(t.fare_amount) AS avg_fare
FROM trips_large t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY num_trips DESC;

SELECT
  z.Borough,
  COUNT(*) AS num_trips,
  AVG(t.fare_amount) AS avg_fare
FROM trips_medium t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY num_trips DESC;

SELECT
  z.Borough,
  COUNT(*) AS num_trips,
  AVG(t.fare_amount) AS avg_fare
FROM trips_small t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY num_trips DESC;
