CREATE OR REPLACE TABLE STAGED.ALERTS AS
SELECT
  r.id AS alert_id,
  NULLIF(UPPER(trim(r.parkCode)), '') AS park_code,
  NULLIF(trim(r.url), '') AS url, 
  r.title,
  r.description,
  r.category,
  e.related_event_id,
  e.related_event_title,
  e.related_event_url,
  r.lastIndexedDate
FROM RAW.ALERTS r
LEFT JOIN LATERAL (
  SELECT
    (t.value).id AS related_event_id,
    (t.value).title AS related_event_title,
    (t.value).url AS related_event_url
  FROM UNNEST(r.relatedRoadEvents) AS t(value)
  LIMIT 1
) e ON TRUE
WHERE NULLIF(UPPER(trim(r.parkCode)), '') IS NOT NULL;