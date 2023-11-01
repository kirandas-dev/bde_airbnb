/*
This SQL code defines a snapshot of the room listings data in the BDE_AirBnB project. The snapshot is stored in the 'raw' schema and uses the 'listing_id' column as the unique key. The snapshot strategy is set to 'timestamp' and the 'SCRAPED_DATE' column is used to track updates.

The code selects distinct values of 'SCRAPED_DATE', 'host_id', 'listing_id', and 'room_type' from the 'listings' table in the 'raw' schema and stores them in a temporary table called 'room'. Finally, the code selects all columns from the 'room' table.


*/
{% snapshot room_snapshot %}
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
room as (
    select DISTINCT
        SCRAPED_DATE,
        host_id,
        listing_id,
        room_type
        
    from source
)
select * from room
{% endsnapshot %}
