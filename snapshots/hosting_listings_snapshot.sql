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