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


stg_host AS (
    SELECT
        h.scraped_date AS date,
        h.host_id,
        h.host_name,
        h.host_since,
        h.host_is_superhost,
        COALESCE(
            CASE
                WHEN h.host_neighbourhood = '' THEN s.suburb_name
                ELSE h.host_neighbourhood
            END,
            s.suburb_name
        ) AS host_neighbourhood,
        h.dbt_scd_id,
        h.dbt_updated_at,
        h.dbt_valid_from,
        h.dbt_valid_to
    FROM handle_nulls h
    LEFT JOIN (
        SELECT DISTINCT ON (p.host_id)
            p.host_id,
            s.suburb_name
        FROM {{ ref('property_snapshot') }} p
        JOIN {{ ref('stg_suburb') }} s ON s.lga_name = p.listing_neighbourhood
    ) s ON h.host_id = s.host_id
)



-- Select the final result
select * from stg_host
