/*
This script selects all columns from the 'stg_property' table and loads them into the 'dim_property' table in the 'warehouse' schema.
*/

{{ config(
    target_schema='warehouse'
) }}

select * from {{ ref('stg_property') }}
