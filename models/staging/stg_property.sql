{{ config(
   
    target_schema='staging'
) }}

with source as (
    select *
    from {{ ref('property_snapshot') }}
),

 stg_property AS (
    SELECT
        SCRAPED_DATE as date,
        listing_id,
        host_id,
        FIRST_VALUE(listing_neighbourhood) OVER (PARTITION BY listing_id ORDER BY SCRAPED_DATE) AS listing_neighbourhood,
        property_type,
        room_type
    FROM source
)
SELECT * FROM stg_property