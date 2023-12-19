/*
This query retrieves the top 5 neighborhoods, property types, room types, and accommodates based on the following criteria:
- Listings with availability
- Listings with non-null review scores
The results are sorted by:
- Average review scores rating in descending order
- Total number of reviews in descending order
- Total stays in descending order
- Average estimated revenue in descending order
*/

SELECT
    t.listing_neighbourhood,
    t.property_type,
    t.room_type,
    t.accommodates,
    SUM(t.stays) AS total_stays,
    SUM(t.estimated_revenue) AS total_estimated_revenue,
    AVG(t.review_scores_rating) AS avg_review_scores_rating,
    SUM(t.number_of_reviews) AS total_number_of_reviews
FROM (
    SELECT
        f.listing_neighbourhood,
        f.property_type,
        f.room_type,
        f.accommodates,
        f.number_of_reviews,
        f.review_scores_rating,
        f.price,
        30 - f.availability_30 AS stays,
        f.price * (30 - f.availability_30) AS estimated_revenue
    FROM
        public_warehouse.facts_listing f
    WHERE
        f.has_availability = 't'
        AND f.review_scores_rating IS NOT NULL -- Filter out listings with missing review scores
) AS t
GROUP BY
    t.listing_neighbourhood,
    t.property_type,
    t.room_type,
    t.accommodates
ORDER BY
     AVG(t.review_scores_rating) DESC,
    SUM(t.number_of_reviews) desc,
    SUM(t.stays) desc,
	AVG(t.estimated_revenue) DESC
   
LIMIT 5;