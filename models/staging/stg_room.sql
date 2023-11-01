/*
This SQL script updates the dbt_valid_to column in the raw.room_snapshot table by setting it to the next scraped_date value for each host_id. It then selects data from the staging.stg_room table by casting host_id and listing_id to integer, and setting dbt_valid_to to '9999-12-31' if it is null. The data is selected from the room_snapshot table using the ref() function.
*/

-- Update raw.room_snapshot  t1
-- set dbt_valid_to = next_date
-- from (
-- select host_id, scraped_date, lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
-- from raw.room_snapshot) t2
-- where t1.host_id=t2.host_id and t1.scraped_date = t2.scraped_date;

{{
    config(
      target_schema='staging'
    
       )
}}

with stg_room as (
    select to_date(SCRAPED_DATE, 'YYYY-MM-DD') as date,
        CAST(host_id AS INT) as host_id, -- Cast host_id to integer
        CAST(listing_id AS INT) as listing_id, -- Cast listing_id to integer
        room_type,
        dbt_valid_from,
         COALESCE(
            CASE
                WHEN dbt_valid_to IS NULL THEN '9999-12-31' 
                ELSE dbt_valid_to
            END
        ) AS dbt_valid_to
    FROM {{ ref('room_snapshot') }} 
)

select * from stg_room