CREATE OR REPLACE TABLE CURATED.NPS_PARK_USAGE_ANNUAL AS
SELECT
    p.id AS park_id,
    p.name AS park_name,
    pu.Year,
    pu.Month,
    pu.RecreationVisits,
    pu.NonRecreationVisits,
    pu.ConcessionerCamping,
    pu.TentCampers,
    pu.RVCampers
FROM STAGED.PARKS p
LEFT JOIN STAGED.PUBLIC_USE pu ON p.park_code = pu.UnitCode;