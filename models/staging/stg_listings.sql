{{
    config(
        target_schema='staging',
      
                )
}}
 
with stg_listings as (
    select * from {{ source('raw', 'listings') }}
)
 
SELECT
    CAST(host_id AS INT) as host_id, -- Cast host_id to integer
    CAST(listing_id AS INT) as listing_id, -- Cast listing_id to integer
    to_date(scraped_date, 'YYYY-MM-DD') as date,
    price,
    accommodates,
    has_availability,
    availability_30, 
    COALESCE(NULLIF(number_of_reviews, NULL), 0) AS number_of_reviews,
    CASE
        WHEN review_scores_rating = 'NaN' THEN 0
        ELSE COALESCE(review_scores_rating::numeric, 0)
    END AS review_scores_rating
FROM stg_listings


