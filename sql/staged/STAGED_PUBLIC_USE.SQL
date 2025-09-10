CREATE OR REPLACE TABLE STAGED.PUBLIC_USE AS
SELECT
    ParkName,
    UnitCode,
    ParkType,
    Region,
    State,
    Year,
    Month,
    CAST(REPLACE(RecreationVisits, ',', '') AS BIGINT) AS RecreationVisits,
    CAST(REPLACE(NonRecreationVisits, ',', '') AS BIGINT) AS NonRecreationVisits,
    CAST(REPLACE(ConcessionerCamping, ',', '') AS BIGINT) AS ConcessionerCamping,
    CAST(REPLACE(TentCampers, ',', '') AS BIGINT) AS TentCampers,
    CAST(REPLACE(RVCampers, ',', '') AS BIGINT) AS RVCampers
FROM RAW.PUBLIC_USE
