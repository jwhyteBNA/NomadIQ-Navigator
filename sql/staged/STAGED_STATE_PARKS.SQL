CREATE OR REPLACE TABLE STAGED.STATE_PARKS AS
SELECT
    objectid AS id,
    "Park Name" AS park_name,
    "Visitors 2016" AS visitors_2016,
    "Acres" AS acres,
    "Year Established" AS year_established,
    CASE
        WHEN function = 'R' THEN 'Recreation'
        WHEN function = 'H' THEN 'Historical'
        WHEN function = 'S' THEN 'Scenic'
        ELSE NULL
    END AS function,
    CASE WHEN Camping = 'Y' THEN TRUE ELSE FALSE END AS camping_available,
    CASE WHEN Boating = 'Y' THEN TRUE ELSE FALSE END AS boating_available,
    CASE WHEN "Biking and/or Hiking" = 'Y' THEN TRUE ELSE FALSE END AS biking_hiking_available,
    CASE WHEN Fishing = 'Y' THEN TRUE ELSE FALSE END AS fishing_available,
    CASE WHEN Golf = 'Y' THEN TRUE ELSE FALSE END AS golf_available,
    CASE WHEN Equestrian = 'Y' THEN TRUE ELSE FALSE END AS equestrian_available,
    CASE WHEN OHV = 'Y' THEN TRUE ELSE FALSE END AS ohv_available,
    CASE WHEN "Winter Recreation" = 'Y' THEN TRUE ELSE FALSE END AS winter_recreation_available,
    CASE WHEN Wildlife = 'Y' THEN TRUE ELSE FALSE END AS wildlife_available,
    Region AS region,
    parkabbid AS park_abbreviation,
    website,
    CASE WHEN Museums = 'Y' THEN TRUE ELSE FALSE END AS museums_available,
    GlobalID AS global_id,
    Lat AS latitude,
    Long AS longitude,
    "Street Address" AS street_address,
    City AS city,
    Zip AS zip,
    State AS state,
    Remarks AS remarks
FROM RAW.STATE_PARKS;