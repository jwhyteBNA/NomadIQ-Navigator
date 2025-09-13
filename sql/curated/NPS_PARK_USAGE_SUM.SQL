CREATE OR REPLACE TABLE CURATED.PARK_USAGE_SUMMARIZED AS
SELECT
    p.id AS park_id,
    p.name AS park_name,
    pu.Year,
    SUM(pu.RecreationVisits) AS total_recreation_visits,
    SUM(pu.NonRecreationVisits) AS total_non_recreation_visits,
    SUM(pu.ConcessionerCamping) AS total_concessioner_camping,
    SUM(pu.TentCampers) AS total_tent_campers,
    SUM(pu.RVCampers) AS total_rv_campers
FROM STAGED.PARKS p
LEFT JOIN STAGED.PUBLIC_USE pu ON p.park_code = pu.UnitCode
GROUP BY p.id, p.name, pu.Year
ORDER BY p.name, pu.Year;