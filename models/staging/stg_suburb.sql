{{
    config(
      target_schema='staging'
    
       )
}}

with source as (
    select *
    from {{ source('public', 'nsw_lga_suburb') }}
),
stg_suburb as (
    SELECT INITCAP(suburb_name) AS suburb_name,
      INITCAP(lga_name) AS lga_name
    from source
)
select * from stg_suburb