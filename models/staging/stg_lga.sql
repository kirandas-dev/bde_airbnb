/*
This script is used to create a staging table named "stg_lga" in the "staging" schema. 
The table is created by selecting data from the "nsw_lga_code" table in the "raw" schema and adding a new row with "Overseas" and "0000" as the lga_name and lga_code respectively. 
The lga_name column is transformed to have the first letter of each word capitalized using the INITCAP function. 
The resulting table is used as a staging table for further processing in the ETL pipeline.
*/
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
