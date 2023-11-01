/*
This SQL script selects all columns from the 'stg_G01' table and returns the result. This script is located in the '/Users/tantri/Documents/My Research Material/UTSSpring23/Data Engineering/Assignment_3_BDE_AirBnb/BDE_AirBnB/models/warehouse/dim_G01.sql' file path. The 'config' function sets the target schema to 'warehouse'. The 'ref' function references the 'stg_G01' table.
*/
{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_G01') }}

