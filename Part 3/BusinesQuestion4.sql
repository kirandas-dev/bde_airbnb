/*
This SQL script calculates the total stays and estimated revenue for each host in each neighbourhood, and determines whether the estimated revenue is enough to cover the annualized median mortgage. 

The script first creates a Common Table Expression (CTE) called HostListingMetrics, which calculates the estimated revenue for each listing based on its price and availability. The CTE also joins with the dim_G02 table to get the annualized median mortgage for each neighbourhood.

The main query then aggregates the data from the CTE by host and neighbourhood, and calculates the total stays and estimated revenue for each group. It also calculates whether the estimated revenue is enough to cover the annualized median mortgage, and returns a 'Yes' or 'No' value accordingly.

FILEPATH: /Users/tantri/Desktop/Assignment_3_Kiran_Das/Part 3/BusinesQuestion4.sql
*/
WITH HostListingMetrics AS (
    SELECT
        f.host_id,
        f.listing_neighbourhood,
        f.price,
        30 - f.availability_30 AS stays,
        d.median_mortgage_repay_monthly * 12 AS annualized_median_mortgage,
        f.price * (30 - f.availability_30) AS estimated_revenue
    FROM
        public_warehouse.facts_listing f
    JOIN public_warehouse."dim_G02" d ON f."listing_neighbourhood_lga_code" = d.lga_code
    WHERE
        f.has_availability = 't'
)

SELECT
    host_id,
    listing_neighbourhood,
    SUM(stays) AS total_stays,
    annualized_median_mortgage,
    SUM(estimated_revenue) AS total_estimated_revenue,
    CASE
        WHEN SUM(estimated_revenue) >= annualized_median_mortgage THEN 'Yes'
        ELSE 'No'
    END AS can_cover_mortgage
FROM HostListingMetrics
GROUP BY host_id, listing_neighbourhood, annualized_median_mortgage;


