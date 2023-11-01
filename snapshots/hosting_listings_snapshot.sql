/*
This SQL script creates a snapshot of the host information for each Airbnb listing. The snapshot is stored in the 'raw' schema and uses the 'listing_id' as the unique key. The snapshot is created using a timestamp strategy and is updated based on the 'SCRAPED_DATE' column.

The script first selects all columns from the 'listings' table in the 'raw' schema. It then selects distinct host information from the source table and stores it in the 'host' table. Finally, it selects all columns from the 'host' table.

This script is part of the BDE_AirBnB project for the Data Engineering course at UTS Spring 2023.
*/
{% snapshot host_snapshot %}
{{
    config(
      target_schema='raw',
      unique_key='listing_id',
      strategy='timestamp',
      updated_at='SCRAPED_DATE',
       )
}}


with source as (
    select *
    from {{ source('raw', 'listings') }}
),
host as (
    select DISTINCT
       SCRAPED_DATE,
       HOST_ID,
       listing_id,
       HOST_NAME,
       HOST_SINCE,
       HOST_IS_SUPERHOST,
       HOST_NEIGHBOURHOOD
    from source
)
select * from host
{% endsnapshot %}