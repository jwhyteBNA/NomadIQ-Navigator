CREATE OR REPLACE TABLE STAGED.TOPICS AS
SELECT
    r.id AS park_id,
    t.value.id   AS topic_id,
    t.value.name AS topic_name
FROM RAW.PARKS r,
    UNNEST(r.topics) AS t(value);