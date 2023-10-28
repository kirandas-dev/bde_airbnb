{% macro dbt_macro__update_snapshot(schema, table) %}
-- SQL logic to update the `dbt_valid_to` field
-- Include the entire SQL update command here
Update {{ ref(schema, table) }} t1
set dbt_valid_to = next_date
from (
  select
    row_number() over () as unique_key,
    scraped_date,
    lead(scraped_date) over (partition by scraped_date order by scraped_date) as next_date
  from {{ ref(schema, table) }}
) t2
on t1.scraped_date = t2.scraped_date;
{% endmacro %}



