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

