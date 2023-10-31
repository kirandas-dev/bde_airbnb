{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_property') }}

