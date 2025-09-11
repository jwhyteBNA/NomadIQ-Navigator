CREATE OR REPLACE TABLE CURATED.PARK_ALERTS AS
SELECT
    p.name AS park_name,
    p.park_code,
    a.title AS alert_title,
    a.description AS alert_description,
    a.category AS alert_category,
    a.url AS alert_url,
    a.lastIndexedDate
FROM STAGED.PARKS p
LEFT JOIN STAGED.ALERTS a ON p.park_code = a.park_code
ORDER BY p.park_code;