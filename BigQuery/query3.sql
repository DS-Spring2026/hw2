SELECT t1.total_amount 
FROM data342.taxi t1 
JOIN data342.taxi t2 ON t1.DOLocationID = t2.PULocationID 
WHERE t1.DOLocationID < 10;