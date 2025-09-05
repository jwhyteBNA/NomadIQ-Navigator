CREATE OR REPLACE TABLE STAGED.ACTIVITIES AS
SELECT
    r.id AS park_id,
    a.value.id AS activity_id,
    a.value.name AS activity_name
FROM RAW.PARKS r,
    UNNEST(r.activities) AS a(value);
