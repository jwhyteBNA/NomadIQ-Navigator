CREATE OR REPLACE TABLE STAGED.ENTRANCE_PASSES AS
SELECT
    r.id AS park_id,
    p.value.title       AS title,
    p.value.description AS description,
    p.value.cost        AS cost
FROM RAW.PARKS r,
    UNNEST(r.entrancePasses) AS p(value);