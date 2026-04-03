SELECT t1.total_amount 
FROM data342.nyc_taxi_22_25 t1 
JOIN data342.nyc_taxi_22_25 t2 ON t1.DOLocationID = t2.PULocationID 
WHERE t1.DOLocationID < 10;