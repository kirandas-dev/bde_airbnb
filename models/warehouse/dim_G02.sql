/*
This SQL script selects all columns from the stg_G02 table and loads them into the dim_G02 table in the warehouse schema. This script is part of the BDE_AirBnB project and is used to create the dim_G02 table in the data warehouse. 
*/
{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_G02') }}
