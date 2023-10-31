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
