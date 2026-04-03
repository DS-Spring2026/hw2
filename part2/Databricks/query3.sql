SET use_cached_result = false;
SELECT t1.total_amount 
FROM taxi t1 
JOIN taxi t2 ON t1.DOLocationID = t2.PULocationID 
WHERE t1.DOLocationID < 10;