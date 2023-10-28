{% snapshot room_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='SCRAPED_DATE',
    )
}}
with source as (
    select *
    from {{ source('public', 'listings') }}
),
room as (
    select DISTINCT
        SCRAPED_DATE,
        host_id,
        listing_id,
        room_type,
        accommodates, 
        price 
    from source
)
select * from room
{% endsnapshot %}
