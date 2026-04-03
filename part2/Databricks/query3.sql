SET use_cached_result = false;
SELECT t1.total_amount 
FROM nyc_taxi_22_25 t1 
JOIN nyc_taxi_22_25 t2 ON t1.DOLocationID = t2.PULocationID 
WHERE t1.DOLocationID < 10;