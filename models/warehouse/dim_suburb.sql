/*
This SQL script selects all columns from the 'stg_suburb' table and returns the result. The script is located in the following file path: /Users/tantri/Documents/My Research Material/UTSSpring23/Data Engineering/Assignment_3_BDE_AirBnb/BDE_AirBnB/models/warehouse/dim_suburb.sql. The target schema for this script is 'warehouse'.
*/
{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_suburb') }}

