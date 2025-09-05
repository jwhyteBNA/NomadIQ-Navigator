CREATE OR REPLACE TABLE STAGED.ENTRANCE_FEES AS
SELECT
    r.id AS park_id,
    f.value.title       AS title,
    f.value.description AS description,
    f.value.cost        AS cost
FROM RAW.PARKS r,
    UNNEST(r.entranceFees) AS f(value);