CREATE OR REPLACE TABLE STAGED.NATL_LANDMARKS AS
SELECT
  "Property ID" AS id,
  "Property Name" AS property_name,
  COALESCE("State", 'Not Listed') AS state,
  "County" AS county,
  "City" AS city,
  "Street & Number" AS street_and_number,
  "Status" AS status,
  "Request Type" AS request_type,
  "Acreage of Property" AS acreage_of_property,
  COALESCE(
    CASE
      WHEN "Level of Significance - Local" = 1 THEN 'Local'
      WHEN "Level of Significance - State" = 1 THEN 'State'
      WHEN "Level of Significance - National" = 1 THEN 'National'
      WHEN "Level of Significance - International" = 1 THEN 'International'
      ELSE NULL
    END,
    'Not Indicated'
  ) AS level_of_significance,
  COALESCE("Area of Significance", 'NOT LISTED') AS area_of_significance,
  COALESCE(
    CASE
      WHEN UPPER("Category of Property") = 'BUILDING' THEN 'BUILDING'
      WHEN UPPER("Category of Property") = 'DISTRICT' THEN 'DISTRICT'
      WHEN UPPER("Category of Property") = 'STRUCTURE' THEN 'STRUCTURE'
      WHEN UPPER("Category of Property") = 'SITE' THEN 'SITE'
      WHEN UPPER("Category of Property") = 'OBJECT' THEN 'OBJECT'
      ELSE NULL
    END,
    'MISC'
  ) AS category_of_property,
  "Listed Date" AS listed_date,
  "Other Names" AS other_names,
  "External Link" AS external_link
FROM RAW.NATIONAL_REGISTER_LANDMARKS
WHERE "Restricted Address" <> True;