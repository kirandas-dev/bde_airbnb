
-- Update public.room_snapshot  t1
-- set dbt_valid_to = next_date
-- from (
-- select host_id, scraped_date, lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
-- from public.room_snapshot) t2
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