CREATE OR REPLACE TABLE STAGED.ADDRESSES AS
SELECT
    r.id AS park_id,
    NULLIF(UPPER(trim(r.parkCode)), '') AS park_code,
    a.value.line1      AS line1,
    a.value.line2      AS line2,
    a.value.line3      AS line3,
    a.value.city       AS city,
    a.value.stateCode  AS state,
    a.value.postalCode AS postal,
    a.value.countryCode AS country,
    a.value.type       AS address_type
FROM RAW.PARKS r,
    UNNEST(r.addresses) AS a(value);

