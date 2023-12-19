/*
This query selects distinct host_id, listing_id, host_neighbourhood_lga_name, and listing_neighbourhood from the facts_listing table in the public_warehouse schema. The selection is limited to host_ids that have more than one distinct listing_id and have a non-null host_neighbourhood in the listings table in the public schema.
*/
select distinct
    f."host_id",
    f."listing_id",
    f."host_neighbourhood_lga_name",
    f."listing_neighbourhood"
FROM public_warehouse.facts_listing f
WHERE f."host_id" IN (
    SELECT "host_id"
    FROM public_warehouse.facts_listing
    GROUP BY "host_id"
    HAVING COUNT(DISTINCT "listing_id") > 1
);