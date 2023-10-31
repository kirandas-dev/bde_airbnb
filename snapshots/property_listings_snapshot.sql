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



