
{% macro update_snapshot(schema, table) %}
-- SQL logic to update the `dbt_valid_to` field
-- Include the entire SQL update command here
Update {{ ref(public, host_snapshot) }} t1
set dbt_valid_to = next_date
from (
  select unique_key, scraped_date, lead(scraped_date) over (partition by unique_key order by scraped_date) as next_date
  from {{ ref(schema, table) }}
) t2
on t1.unique_key = t2.unique_key and t1.scraped_date = t2.scraped_date;
{% endmacro %}
