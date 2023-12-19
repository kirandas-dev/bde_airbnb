/*
This SQL script retrieves the best and worst listing_neighbourhoods based on their average estimated revenue per active listings between May 2020 and April 2021. It then retrieves the corresponding LGA codes for each neighbourhood and adjusts the age groups to be under 30. The result is two tables, one for the best neighbourhood and one for the worst neighbourhood, each containing demographic information for the adjusted age groups.
*/


-- Subquery to find the best listing_neighbourhood and its corresponding LGA code
WITH BestNeighbourhood AS (
    SELECT
        listing_neighbourhood
    FROM public_datamart.dm_listing_neighbourhood
    WHERE DATE_TRUNC('month', "month/year") >= DATE '2020-05-01'
          AND DATE_TRUNC('month', "month/year") <= DATE '2021-04-30'
    GROUP BY listing_neighbourhood
    ORDER BY AVG("Average Estimated Revenue per Active Listings") DESC
    LIMIT 1
)
-- CTE to retrieve the LGA code for the best listing_neighbourhood
, BestLGACode AS (
    SELECT LGA.lga_code,
    B.listing_neighbourhood
    FROM public_warehouse.dim_lga LGA
    INNER JOIN BestNeighbourhood B ON LGA.lga_name = B.listing_neighbourhood
)
-- Adjust age groups to be under 30 using the dynamically retrieved LGA code
select
    BestLGACode.listing_neighbourhood,
    G01.lga_code,
    G01.tot_p_m,
    G01.tot_p_f,
    G01.age_0_4_yr_m,
    G01.age_0_4_yr_f,
    G01.age_5_14_yr_m,
    G01.age_5_14_yr_f,
    G01.age_15_19_yr_m,
    G01.age_15_19_yr_f,
    G01.age_20_24_yr_m,
    G01.age_20_24_yr_f,
    (G01.age_25_34_yr_m / 2) AS age_25_29_yr_m,
    (G01.age_25_34_yr_f / 2) AS age_25_29_yr_f
FROM public_warehouse."dim_G01" G01
INNER JOIN BestLGACode ON G01.lga_code = BestLGACode.lga_code;


-- Subquery to find the worst listing_neighbourhood and its corresponding LGA code
WITH WorstNeighbourhood AS (
    SELECT
        listing_neighbourhood
    FROM public_datamart.dm_listing_neighbourhood
    WHERE DATE_TRUNC('month', "month/year") >= DATE '2020-05-01'
          AND DATE_TRUNC('month', "month/year") <= DATE '2021-04-30'
    GROUP BY listing_neighbourhood
    ORDER BY AVG("Average Estimated Revenue per Active Listings") ASC
    LIMIT 1
)
-- CTE to retrieve the LGA code for the worst listing_neighbourhood
, WorstLGACode AS (
    SELECT LGA.lga_code,
    B.listing_neighbourhood
    FROM public_warehouse.dim_lga LGA
    INNER JOIN WorstNeighbourhood B ON LGA.lga_name = B.listing_neighbourhood
)
-- Adjust age groups to be under 30 using the dynamically retrieved LGA code
select
    WorstLGACode.listing_neighbourhood,
    G01.lga_code,
    G01.tot_p_m,
    G01.tot_p_f,
    G01.age_0_4_yr_m,
    G01.age_0_4_yr_f,
    G01.age_5_14_yr_m,
    G01.age_5_14_yr_f,
    G01.age_15_19_yr_m,
    G01.age_15_19_yr_f,
    G01.age_20_24_yr_m,
    G01.age_20_24_yr_f,
    (G01.age_25_34_yr_m / 2) AS age_25_29_yr_m,
    (G01.age_25_34_yr_f / 2) AS age_25_29_yr_f
    
FROM public_warehouse."dim_G01" G01
INNER JOIN WorstLGACode ON G01.lga_code = WorstLGACode.lga_code;







