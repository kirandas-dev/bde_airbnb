/*
This SQL script selects data from the 'listings' table in the 'raw' schema and casts the 'host_id' and 'listing_id' columns to integers. It also converts the 'scraped_date' column to a date format of 'YYYY-MM-DD'. The 'number_of_reviews' column is replaced with 0 if it is null, and the 'review_scores_rating' column is replaced with 0 if it is 'NaN' or null. The resulting data is then selected and returned.
*/
{{
    config(
        target_schema='staging',
      
                )
}}
 
WITH stg_listings as (
    SELECT * FROM {{ source('raw', 'listings') }}
),
listing_stats AS (
    SELECT
        listing_id AS listing_id_stats, -- Use an alias to disambiguate the column name
        AVG(price) AS mean_price,
        STDDEV(price) AS stddev_price
    FROM stg_listings
    GROUP BY listing_id
)
 
SELECT
    CAST(host_id AS INT) as host_id,
    CAST(stg.listing_id AS INT) as listing_id, -- Qualify the column with the subquery name
    to_date(scraped_date, 'YYYY-MM-DD') as date,
    CASE
        WHEN ABS(price - listing_stats.mean_price) <= 1.5 * listing_stats.stddev_price THEN price
        ELSE listing_stats.mean_price
    END AS price,
    accommodates,
    has_availability,
    availability_30, 
    COALESCE(NULLIF(number_of_reviews, NULL), 0) AS number_of_reviews,
    CASE
        WHEN review_scores_rating = 'NaN' THEN 0
        ELSE COALESCE(review_scores_rating::numeric, 0)
    END AS review_scores_rating
FROM stg_listings AS stg -- Use an alias for the main table
JOIN listing_stats ON stg.listing_id = listing_stats.listing_id_stats