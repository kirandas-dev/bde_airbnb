/*
This script selects all columns from the stg_lga table and creates a new table in the warehouse schema. This table is used as a dimension table in the AirBnB data model. 
*/
{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_lga') }}
