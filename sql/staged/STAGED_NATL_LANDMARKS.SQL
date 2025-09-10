CREATE OR REPLACE TABLE STAGED.NATL_LANDMARKS AS
SELECT
  "Property ID" AS id,
  "Property Name" AS property_name,
  "State" AS state,
  "County" AS county,
  "City" AS city,
  "Street & Number" AS street_and_number,
  "Restricted Address" AS restricted_address,
  "Status" AS status,
  "Request Type" AS request_type,
  "Acreage of Property" AS acreage_of_property,
  CASE
    WHEN "Level of Significance - Local" THEN 'Local'
    WHEN "Level of Significance - State" THEN 'State'
    WHEN "Level of Significance - National" THEN 'National'
    WHEN "Level of Significance - International" THEN 'International'
    WHEN "Level of Significance - Not Indicated" THEN 'Not Indicated'
    ELSE NULL
  END AS level_of_significance,
  "Area of Significance" AS area_of_significance,
  "Category of Property" AS category_of_property,
  "Listed Date" AS listed_date,
  "Other Names" AS other_names,
  "External Link" AS external_link
FROM RAW.NATIONAL_REGISTER_LANDMARKS;