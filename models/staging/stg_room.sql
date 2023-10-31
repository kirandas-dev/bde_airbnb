{{
    config(
      target_schema='staging'
    
       )
}}

with stg_room as (
    select SCRAPED_DATE as date,
        host_id,
        listing_id,
        room_type
    FROM {{ ref('room_snapshot') }} 
)

select * from stg_room