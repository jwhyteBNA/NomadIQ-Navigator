CREATE OR REPLACE TABLE CURATED.NPS_DISTANCES AS
SELECT
    np1.name AS starting_national_park,
    np2.name AS destination_national_park,
    ROUND(
        3959 * acos(
            cos(radians(np1.latitude)) * cos(radians(np2.latitude)) *
            cos(radians(np2.longitude) - radians(np1.longitude)) +
            sin(radians(np1.latitude)) * sin(radians(np2.latitude))
        ), 1
    ) AS distance_miles
FROM STAGED.PARKS np1
JOIN STAGED.PARKS np2
  ON np1.id <> np2.id
WHERE
    3959 * acos(
        cos(radians(np1.latitude)) * cos(radians(np2.latitude)) *
        cos(radians(np2.longitude) - radians(np1.longitude)) +
        sin(radians(np1.latitude)) * sin(radians(np2.latitude))
    ) <= 250
ORDER BY starting_national_park, distance_miles ASC;