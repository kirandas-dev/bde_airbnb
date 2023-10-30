{{ config(
   
    target_schema='staging'
) }}

-- Define a CTE to standardize host_since values
with source as (
    select *
    from {{ ref('host_snapshot') }}
),

-- Define a CTE to standardize host_since
standardize_host_since as (
    select
        CASE
            WHEN host_since IS NULL OR host_since !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN '01/01/1900' ELSE host_since END as host_since,
        scraped_date,
        host_id,
        host_name,
        host_is_superhost,
        host_neighbourhood,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from source
),



-- Define another CTE to format host_since as 'YYYY-MM-DD'
handle_nulls as (
    select
        scraped_date,
        host_id,
        host_name,
        TO_CHAR(TO_DATE(host_since, 'DD/MM/YYYY'), 'YYYY-MM-DD') as host_since,
        COALESCE(NULLIF(host_is_superhost, 'NaN'), MAX(COALESCE(NULLIF(host_is_superhost, 'NaN'), '')) OVER (PARTITION BY host_id)) AS host_is_superhost,
        COALESCE(NULLIF(host_neighbourhood, 'NaN'), MAX(COALESCE(NULLIF(host_neighbourhood, 'NaN'), '')) OVER (PARTITION BY host_id)) AS host_neighbourhood,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from standardize_host_since
),


-- Fill missing values in host_neighbourhood
fill_missing_values as (
    SELECT
        h.scraped_date,
        h.host_id,
        h.host_name,
        host_since,
        host_is_superhost,
        CASE WHEN h.host_neighbourhood = '' THEN (
            SELECT suburb_name
            FROM {{ ref('stg_suburb') }} s
            JOIN {{ ref('property_snapshot') }} p ON s.lga_name = p.listing_neighbourhood
            WHERE p.host_id = h.host_id 
            ORDER BY RANDOM()
            LIMIT 1
        )
        ELSE h.host_neighbourhood
        END AS host_neighbourhood,
        h.dbt_scd_id,
        h.dbt_updated_at,
        h.dbt_valid_from,
        h.dbt_valid_to
    FROM handle_nulls h
)



-- Select the final result
select * from fill_missing_values
