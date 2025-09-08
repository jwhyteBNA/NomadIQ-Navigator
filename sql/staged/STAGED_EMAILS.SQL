CREATE OR REPLACE TABLE STAGED.EMAILS AS
SELECT
    r.id AS park_id,
    e.value.emailAddress AS email_address
FROM RAW.PARKS r,
    UNNEST(r."contacts.emailAddresses") AS e(value);