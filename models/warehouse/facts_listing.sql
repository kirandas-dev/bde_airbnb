{{ config(
   target_schema='warehouse'
) }}

with check_dimensions as (
    select
        listing_id,
		accommodates,
		has_availability,
		availability_30,
		number_of_reviews,
		review_scores_rating,
        date,
        case when host_id in (select distinct host_id from {{ ref('stg_host') }}) then host_id else 0 end as host_id,
        price
    from {{ ref('stg_listings') }}
)

select
    a.listing_id,
    a.date,
    b.listing_neighbourhood,
    a.host_id,
	d.host_name,
	d.host_since,
	d.host_is_superhost,
	d.host_neighbourhood,
    a.price,
    e.lga_code as host_neigbourhood_lga_code,
	c.lga_name as host_neighbourhood_lga_name,
	b.property_type,
	f.room_type,
	a.accommodates,
	a.has_availability,
	a.availability_30,
	a.number_of_reviews,
	a.review_scores_rating
from check_dimensions a
left join {{ ref('stg_property') }} b  on a.listing_id = b.listing_id and a.host_id = b.host_id and a.date::timestamp >= b.dbt_valid_from::timestamp and a.date::timestamp < coalesce(b.dbt_valid_to::timestamp, '9999-12-31'::timestamp)
left join {{ ref('stg_host') }} d  on a.host_id = d.host_id and  a.listing_id = d.listing_id and a.date::timestamp >= d.dbt_valid_from::timestamp and a.date::timestamp < coalesce(d.dbt_valid_to::timestamp, '9999-12-31'::timestamp)
left join {{ ref('stg_suburb') }} c  on d.host_neighbourhood = c.suburb_name
left join {{ ref('stg_lga') }} e  on e.lga_name = c.lga_name
left join {{ ref('stg_room') }} f  on a.host_id = f.host_id and a.listing_id = f.listing_id and a.date::timestamp >= f.dbt_valid_from::timestamp and a.date::timestamp < coalesce(f.dbt_valid_to::timestamp, '9999-12-31'::timestamp)


