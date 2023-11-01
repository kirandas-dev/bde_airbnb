/*
Calculates the estimated revenue and number of distinct hosts for each host neighbourhood and month/year combination. 
The query uses the fact table 'facts_listing' and the dimension tables 'dim_suburb' and 'dim_lga' to join the data. 
The results are grouped by host neighbourhood, month/year and aggregated to calculate the total number of distinct hosts, 
estimated revenue and estimated revenue per host (distinct) for each group. 
*/
-- FILEPATH: /Users/tantri/Documents/My Research Material/UTSSpring23/Data Engineering/Assignment_3_BDE_AirBnb/BDE_AirBnB/models/datamart/dm_host_neighbourhood.sql
{{ config(
    target_schema='datamart'
) }}

-- Calculate metrics for revenue and hosts
WITH RevenueAndHosts AS (
    SELECT
        COALESCE(s.lga_name, f.host_neighbourhood) AS host_neighbourhood_lga,
        DATE_TRUNC('month', f."date") AS "month/year",
        COUNT(DISTINCT f.host_id) AS "Number of Distinct Hosts",
        SUM(f.price) AS "Estimated Revenue"
    FROM {{ ref('facts_listing') }} f
    LEFT JOIN {{ ref('dim_suburb') }} s ON f.host_neighbourhood = s.suburb_name
    LEFT JOIN {{ ref('dim_lga') }} l ON l.lga_name = s.lga_name
    WHERE l.lga_name IS NOT NULL
    GROUP BY COALESCE(s.lga_name, f.host_neighbourhood), DATE_TRUNC('month', f."date")
)

-- Combine the results into one table
SELECT
    rah.host_neighbourhood_lga,
    rah."month/year",
    SUM(rah."Number of Distinct Hosts") AS "Number of Distinct Hosts",
    SUM(rah."Estimated Revenue") AS "Estimated Revenue",
    SUM(rah."Estimated Revenue") / NULLIF(SUM(rah."Number of Distinct Hosts"), 0) AS "Estimated Revenue per Host (distinct)"
FROM RevenueAndHosts rah
GROUP BY rah.host_neighbourhood_lga, rah."month/year"
