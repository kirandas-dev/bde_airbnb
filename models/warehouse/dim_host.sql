/*
This SQL script selects all columns from the 'stg_host' table and returns the result. The script is located at /Users/tantri/Documents/My Research Material/UTSSpring23/Data Engineering/Assignment_3_BDE_AirBnb/BDE_AirBnB/models/warehouse/dim_host.sql and is part of the BDE_AirBnB project. The target schema for this script is 'warehouse'.
*/
{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_host') }}

