{% snapshot property_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='listing_id',
      strategy='timestamp',
      updated_at='SCRAPED_DATE',
    )
}}
with source as (
    select *
    from {{ source('public', 'listings') }}
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
