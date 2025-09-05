CREATE OR REPLACE TABLE STAGED.ALERTS AS
SELECT
    r.id AS alert_id,
    r.url,
    r.title,
    r.parkCode,
    r.description,
    r.category,
    e.value.url AS related_road_event_url,
    r.lastIndexedDate
FROM RAW.ALERTS r
LEFT JOIN UNNEST(r.relatedRoadEvents) AS e(value) ON TRUE;