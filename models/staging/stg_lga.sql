{{
    config(
      target_schema='staging'
    
       )
}}

with source as (
    select *
    from {{ source('public', 'nsw_lga_code') }}
),
stg_lga as (
    SELECT INITCAP(lga_name) AS lga_name, lga_code
    from source
)
select * from stg_lga