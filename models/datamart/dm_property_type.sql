/*
This SQL script generates a datamart table that contains various metrics for Airbnb listings based on their property type, room type, and accommodates. The metrics include active listings rate, minimum price, maximum price, median price, average price, number of distinct hosts, superhost rate, average review scores rating, percentage change for active listings, percentage change for inactive listings, total number of stays, and average estimated revenue per active listings. The script reads from a fact table called facts_listing and writes to a datamart schema. 
*/
{{ config(
   
    target_schema='datamart'
) }}

WITH Listings AS (
    SELECT
        "property_type",
        "room_type",
        "accommodates",
        DATE_TRUNC('month', "date") AS "month/year",
        "has_availability",
        "price",
        "host_id",
        "host_is_superhost",
        "review_scores_rating",
        "listing_id",
        30 - "availability_30" AS "stays"
    FROM {{ ref('facts_listing') }}
),
Property_Metrics AS (
    SELECT
        "property_type",
        "room_type",
        "accommodates",
        "month/year",
        COUNT( CASE WHEN "has_availability" = 't' THEN "listing_id" END) AS "Active Listings Rate",
        MIN("price") FILTER (WHERE "has_availability" = 't') AS "Minimum Price",
        MAX("price") FILTER (WHERE "has_availability" = 't') AS "Maximum Price",
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "price") FILTER (WHERE "has_availability" = 't') AS "Median Price",
        AVG("price") FILTER (WHERE "has_availability" = 't') AS "Average Price",
        COUNT(DISTINCT "host_id") FILTER (WHERE "has_availability" = 't') AS "Number of Distinct Hosts",
        NULLIF(COUNT(DISTINCT CASE WHEN "host_is_superhost" = 't' THEN "host_id" END), 0) * 100.0 / NULLIF(COUNT(DISTINCT "host_id"), 0) AS "Superhost Rate",
        AVG("review_scores_rating") FILTER (WHERE "has_availability" = 't') AS "Average Review Scores Rating",
        COUNT(*) FILTER (WHERE "has_availability" = 't')  as active_currentcount,
        LAG(COUNT(*) FILTER (WHERE "has_availability" = 't')) OVER (PARTITION BY "property_type", "room_type", "accommodates" ORDER BY "month/year") as active_previouscount,
        COUNT(*) FILTER (WHERE "has_availability" = 'f')  as inactive_currentcount,
        LAG(COUNT(*) FILTER (WHERE "has_availability" = 'f')) OVER (PARTITION BY "property_type", "room_type", "accommodates"  ORDER BY "month/year") as inactive_previouscount,
        SUM("stays") AS "Total Number of Stays",
        NULLIF(SUM("stays" * "price"), 0) / NULLIF(COUNT(*), 0) AS "Average Estimated Revenue per Active Listings"
    FROM Listings
    GROUP BY "property_type", "room_type", "accommodates", "month/year"
)
SELECT
    "property_type",
    "room_type",
    "accommodates",
    "month/year",
    "Active Listings Rate",
    "Minimum Price",
    "Maximum Price",
    "Median Price",
    "Average Price",
    "Number of Distinct Hosts",
    "Superhost Rate",
    "Average Review Scores Rating",
    ((active_currentcount - active_previouscount) * 100.0) / NULLIF(active_previouscount, 0) AS "Percentage change for active listings",
    ((inactive_currentcount - inactive_previouscount) * 100.0) / NULLIF(inactive_previouscount, 0) AS "Percentage change for inactive listings",
    "Total Number of Stays",
    "Average Estimated Revenue per Active Listings"
FROM Property_Metrics
