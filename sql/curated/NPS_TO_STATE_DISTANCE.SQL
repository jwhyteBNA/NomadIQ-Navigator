CREATE OR REPLACE TABLE CURATED.NPS_TO_STATE_DISTANCE AS
SELECT
    np.park_code AS national_park_code,
    np.name AS national_park_name,
    sp.park_name AS state_park_name,
    ROUND(
            3959 * acos(
                    cos(radians(np.latitude)) * cos(radians(sp.latitude)) *
                    cos(radians(sp.longitude) - radians(np.longitude)) +
                    sin(radians(np.latitude)) * sin(radians(sp.latitude))
                   ), 1
    )AS distance_miles
FROM STAGED.PARKS np
JOIN STAGED.STATE_PARKS sp
ON 3959 * acos(
        cos(radians(np.latitude)) * cos(radians(sp.latitude)) *
        cos(radians(sp.longitude) - radians(np.longitude)) +
        sin(radians(np.latitude)) * sin(radians(sp.latitude))
    ) < 150
ORDER BY np.park_code, distance_miles ASC;