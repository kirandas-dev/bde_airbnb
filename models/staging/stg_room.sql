{{
    config(
      target_schema='staging'
    
       )
}}

with stg_room as (
    select to_date(SCRAPED_DATE, 'YYYY-MM-DD') as date,
        host_id,
        listing_id,
        room_type,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('room_snapshot') }} 
)

select * from stg_room