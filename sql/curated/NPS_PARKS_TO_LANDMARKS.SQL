CREATE OR REPLACE TABLE CURATED.NPS_PARKS_TO_LANDMARKS AS
SELECT DISTINCT
  p.park_code,
  p.name AS park_name,
  a.city AS park_city,
  a.state AS park_state,
  nl.property_name,
  nl.street_and_number AS landmark_address,
  nl.city AS landmark_city,
  nl.state AS landmark_state,
  nl.county AS landmark_county,
  nl.listed_date,
  nl.level_of_significance,
  nl.area_of_significance,
  nl.category_of_property
FROM STAGED.PARKS p
LEFT JOIN STAGED.ADDRESSES a ON p.id = a.park_id
INNER JOIN STAGED.NATL_LANDMARKS nl ON 1=1
INNER JOIN STAGED.STATE_ABBREVIATIONS sa ON LOWER(TRIM(nl.state)) = LOWER(TRIM(sa.full_name))
WHERE POSITION(sa.abbr IN a.state) > 0
ORDER BY p.name, nl.property_name;