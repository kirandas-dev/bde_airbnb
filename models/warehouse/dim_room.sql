/*
This script selects all columns from the stg_room table and loads them into the dim_room table in the warehouse schema. 
*/

{{ config(
    target_schema='warehouse'
) }}

select * from {{ ref('stg_room') }}
