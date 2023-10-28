{% snapshot host_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='HOST_ID',
      strategy='timestamp',
      updated_at='SCRAPED_DATE',
       )
}}


with source as (
    select *
    from {{ source('public', 'listings') }}
),
host as (
    select DISTINCT
       SCRAPED_DATE,
       HOST_ID,
       HOST_NAME,
       HOST_SINCE,
       HOST_IS_SUPERHOST,
       HOST_NEIGHBOURHOOD
    from source
)
select * from host
{% endsnapshot %}