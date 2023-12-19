-- This SQL script creates a table named "listings" in the "raw" schema. The table has columns for various attributes of a property listing, such as the listing ID, host ID, property type, room type, price, and review scores. This table can be used to store raw data scraped from a website or API related to property listings.
--int4 had to be changed to int 8 to handle the large numbers
CREATE TABLE raw.listings (
	listing_id int8 null,
	scrape_id int8 NULL,
	scraped_date varchar(500) NULL,
	host_id int4 NULL,
	host_name varchar(500) NULL,
	host_since varchar(500) NULL,
	host_is_superhost varchar(500) NULL,
	host_neighbourhood varchar(500) NULL,
	listing_neighbourhood varchar(500) NULL,
	property_type varchar(500) NULL,
	room_type varchar(500) NULL,
	accommodates int4 NULL,
	price int4 NULL,
	has_availability varchar(500) NULL,
	availability_30 int4 NULL,
	number_of_reviews int4 NULL,
	review_scores_rating double precision NULL,
    review_scores_accuracy double precision NULL,
    review_scores_cleanliness double precision NULL,
    review_scores_checkin double precision NULL,
    review_scores_communication double precision NULL,
    review_scores_value double precision NULL
);