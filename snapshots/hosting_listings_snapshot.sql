{% snapshot host_snapshot %}
{{
    config(
      target_schema='public',
      unique_key='HOST_ID',
      strategy='timestamp',
      updated_at='SCRAPED_DATE'
    )
}}

select * from {{ source('public', 'listings') }}
{% endsnapshot %}

-- After the snapshot, call the custom macro to update `dbt_valid_to`
{{ update_snapshot('public', 'host_snapshot') }}
