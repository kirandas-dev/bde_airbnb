-- This SQL script creates a table named "2016Census_G02_NSW_LGA" in the "raw" schema. The table has columns for various demographic data such as median age, median mortgage repayments, median personal income, median rent, median family income, average number of persons per bedroom, median household income, and average household size. The data is sourced from the 2016 Australian Census and is specific to Local Government Areas (LGAs) in New South Wales (NSW).
CREATE TABLE raw."2016Census_G02_NSW_LGA" (
	lga_code_2016 varchar(500) NULL,
	median_age_persons int4 NULL,
	median_mortgage_repay_monthly int4 NULL,
	median_tot_prsnl_inc_weekly int4 NULL,
	median_rent_weekly int4 NULL,
	median_tot_fam_inc_weekly int4 NULL,
	average_num_psns_per_bedroom float4 NULL,
	median_tot_hhd_inc_weekly int4 NULL,
	average_household_size float4 NULL
);