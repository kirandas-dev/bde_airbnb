{{ config(
   
    target_schema='warehouse'
) }}

select * from {{ ref('stg_listings') }}


with check_dimensions as
(select
	listing_id,
	date,
	case when host_id in (select distinct host_id from {{ ref('stg_host') }}) then host_id else 0 end as host_id,
	price
from {{ ref('stg_listings') }})

select
	a.listing_id,
	a.date,
	b.listing_neighbourhood,
	a.host_id,
	a.price,
	c.lga_code
from check_dimensions a
left join {{ ref('stg_property') }} b  on a.listing_id = b.listing_id and a.date::timestamp >= b.dbt_valid_from and a.date::timestamp < coalesce(b.dbt_valid_to, '9999-12-31'::timestamp)
left join {{ ref('stg_lga') }} c  on a.listing_id = b.listing_id 