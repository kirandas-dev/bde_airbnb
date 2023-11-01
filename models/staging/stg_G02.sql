/*
This script loads data from the '2016Census_G02_NSW_LGA' table in the 'raw' schema and transforms it into the 'stg_G02' table in the 'staging' schema. The transformation involves casting the 'lga_code_2016' column to an integer, and selecting several columns from the source table. The resulting 'stg_G02' table contains the following columns:
- lga_code (integer)
- median_age_persons (float)
- median_mortgage_repay_monthly (float)
- median_tot_prsnl_inc_weekly (float)
- median_rent_weekly (float)
- median_tot_fam_inc_weekly (float)
- average_num_psns_per_bedroom (float)
- median_tot_hhd_inc_weekly (float)
- average_household_size (float)
*/
{{
    config(
      target_schema='staging'
    
       )
}}

with source as (
    select *
    from {{ source('raw', '2016Census_G02_NSW_LGA') }}
),
stg_G02 as (
   SELECT CAST(substring(lga_code_2016 FROM 4) AS INTEGER) AS lga_code, median_age_persons, median_mortgage_repay_monthly, median_tot_prsnl_inc_weekly, median_rent_weekly, median_tot_fam_inc_weekly, average_num_psns_per_bedroom, median_tot_hhd_inc_weekly, average_household_size

FROM source

)
select * from stg_G02
