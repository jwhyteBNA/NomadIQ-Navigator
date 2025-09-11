CREATE OR REPLACE TABLE STAGED.PARKS AS
SELECT
    id,
    UPPER(trim(parkCode)) AS park_code,
    fullName,
    name,
    NULLIF(trim(designation), '') AS designation, 
    description,
    states,
    CAST(NULLIF(latitude, '') AS DOUBLE) AS latitude,
    CAST(NULLIF(longitude, '') AS DOUBLE) AS longitude,
    url,
    directionsInfo,
    directionsUrl,
    weatherInfo
FROM RAW.PARKS;
