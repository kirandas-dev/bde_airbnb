{{ config(
   
    target_schema='staging'
) }}

with source as (
    select *
    from {{ ref('property_snapshot') }}
),

 stg_property AS (
    SELECT
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as date,
        listing_id,
        host_id,
        FIRST_VALUE(listing_neighbourhood) OVER (PARTITION BY listing_id ORDER BY SCRAPED_DATE) AS listing_neighbourhood,
        property_type,
        room_type,
        dbt_valid_from,
        dbt_valid_to
    FROM source
)
SELECT * FROM stg_property