{{
    config(
        target_schema='staging'
    )
}}

with source as (
    select *
    from {{ source('raw', 'nsw_lga_code') }}
),
stg_lga as (
    SELECT INITCAP(lga_name) AS lga_name, lga_code
    from source

    -- Add the new row with "Overseas" and "0"
    UNION ALL
    SELECT 'Overseas' AS lga_name, 0000 AS lga_code
)
select * from stg_lga
