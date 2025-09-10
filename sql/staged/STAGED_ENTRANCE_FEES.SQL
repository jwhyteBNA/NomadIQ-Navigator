CREATE OR REPLACE TABLE STAGED.ENTRANCE_FEES AS
SELECT
    r.id AS park_id,
    f.value.title       AS title,
    f.value.description AS description,
    CAST(f.value.cost AS DECIMAL(10,2)) AS cost
FROM RAW.PARKS r,
    UNNEST(r.entranceFees) AS f(value);