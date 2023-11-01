/*
This SQL code defines a snapshot of the property listings data in the AirBnB database. The snapshot is stored in the 'raw' schema and uses the 'listing_id' column as the unique key. The snapshot strategy is set to 'timestamp' and the 'SCRAPED_DATE' column is used to track updates.

The code selects data from the 'listings' table in the 'raw' schema and creates a new table called 'property' that contains distinct values for the 'SCRAPED_DATE', 'listing_id', 'host_id', 'listing_neighbourhood', 'property_type', 'room_type', and 'accommodates' columns.

The final output of the code is the 'property' table.
*/
{% snapshot property_snapshot %}
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
property as (
    select DISTINCT
        SCRAPED_DATE,
        listing_id,
        host_id,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates 
    from source
)
select * from property
{% endsnapshot %}
