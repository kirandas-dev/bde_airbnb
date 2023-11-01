

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
        host_id,
        price
    from {{ ref('stg_listings') }}
)

select
    a.listing_id,
    a.date,
    b.listing_neighbourhood,
    h.lga_code as listing_neighbourhood_lga_code,
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
left join {{ ref('stg_property') }} b  on a.listing_id = b.listing_id and a.date= b.date and a.host_id = b.host_id and a.date::timestamp >= b.dbt_valid_from::timestamp and a.date::timestamp <= b.dbt_valid_to::timestamp
left join {{ ref('stg_host') }} d  on   a.listing_id = d.listing_id and a.date= d.date and a.host_id = d.host_id  and a.date::timestamp >= d.dbt_valid_from::timestamp and a.date::timestamp <= d.dbt_valid_to::timestamp 
left join {{ ref('stg_suburb') }} c  on d.host_neighbourhood = c.suburb_name
left join {{ ref('stg_lga') }} e  on e.lga_name = c.lga_name
left join {{ ref('stg_room') }} f  on a.listing_id = f.listing_id and a.date= f.date  and a.date::timestamp >= f.dbt_valid_from::timestamp and a.date::timestamp <= f.dbt_valid_to::timestamp 
left join {{ ref('stg_lga') }} h  on h.lga_name = b.listing_neighbourhood