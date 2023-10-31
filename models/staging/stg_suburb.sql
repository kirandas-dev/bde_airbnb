{{
    config(
      target_schema='staging'
    
       )
}}

with source as (
    select *
    from {{ source('raw', 'nsw_lga_suburb') }}
),
stg_suburb as (
    SELECT INITCAP(suburb_name) AS suburb_name,
      INITCAP(lga_name) AS lga_name
    from source
    UNION ALL
    SELECT 'Overseas' AS lga_name, 'Overseas' AS suburb_name
)
select * from stg_suburb