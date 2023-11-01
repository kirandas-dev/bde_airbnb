/*
This SQL script is used to load data from the 'nsw_lga_suburb' table in the 'raw' schema into the 'stg_suburb' table in the 'staging' schema. The script performs the following steps:
1. Capitalizes the first letter of each word in the 'suburb_name' and 'lga_name' columns.
2. Appends a row with 'Overseas' as the 'suburb_name' and 'lga_name' to the 'stg_suburb' table.
The script uses the 'config' function to set the target schema to 'staging'. The 'source' CTE retrieves data from the 'nsw_lga_suburb' table in the 'raw' schema. The 'stg_suburb' CTE performs the data transformation and loads the data into the 'stg_suburb' table. The final SELECT statement retrieves all rows from the 'stg_suburb' table.
*/
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