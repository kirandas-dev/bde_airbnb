/*
This SQL script updates the dbt_valid_to column in the raw.property_snapshot table by setting it to the next scraped_date value for each listing_id. It then selects data from the property_snapshot table and casts host_id and listing_id to integer data types. It also calculates the first value of listing_neighbourhood for each listing_id and sets dbt_valid_to to '9999-12-31' if it is null. The resulting data is then selected from the stg_property table.
*/

-- Update raw.property_snapshot  t1
-- set dbt_valid_to = next_date
-- from (
-- select listing_id , scraped_date, lead(scraped_date) over (partition by listing_id order by scraped_date) as next_date
-- from raw.property_snapshot) t2
-- where t1.listing_id=t2.listing_id and t1.scraped_date = t2.scraped_date;

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
        CAST(host_id AS INT) as host_id, -- Cast host_id to integer
        CAST(listing_id AS INT) as listing_id, -- Cast listing_id to integer
        FIRST_VALUE(listing_neighbourhood) OVER (PARTITION BY listing_id ORDER BY SCRAPED_DATE) AS listing_neighbourhood,
        property_type,
        room_type,
        dbt_valid_from,
         COALESCE(
            CASE
                WHEN dbt_valid_to IS NULL THEN '9999-12-31' 
                ELSE dbt_valid_to
            END
        ) AS dbt_valid_to
    FROM source
)
SELECT * FROM stg_property