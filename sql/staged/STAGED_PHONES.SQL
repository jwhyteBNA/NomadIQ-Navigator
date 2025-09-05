CREATE OR REPLACE TABLE STAGED.PHONES AS
SELECT
    r.id AS park_id,
    p.value.phoneNumber AS phone_number,
    p.value.type        AS phone_type,
    p.value.extension   AS extension
FROM RAW.PARKS r,
    UNNEST(r."contacts.phoneNumbers") AS p(value);