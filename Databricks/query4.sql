SELECT year, VendorID, total_rev,
       sum(total_rev) over(partition by VendorID) gross_total_rev,
       sum(total_rev) over (partition by VendorID order by year) gross_moving_total_rev,
       sum(total_rev) over (partition by VendorID order by year ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) gross_moving_total_rev,
       avg (total_rev) over(partition by VendorID order by year ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) gross_moving_avg_rev,
       avg (total_rev) over(partition by VendorID order by year RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) gross_moving_avg_rev
FROM (
    SELECT extract(year from tpep_pickup_datetime) AS year, VendorID, sum(total_amount) as total_rev
    FROM taxi group by 1,2) foo;
