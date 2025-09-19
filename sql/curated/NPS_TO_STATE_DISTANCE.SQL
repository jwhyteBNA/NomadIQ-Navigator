CREATE OR REPLACE TABLE CURATED.NPS_TO_STATE_DISTANCE AS
SELECT
    np.park_code AS national_park_code,
    np.name AS national_park_name,
    np.latitude AS national_park_latitude,
    np.longitude AS national_park_longitude,
    sp.park_name AS state_park_name,
    ROUND(
            3959 * acos(
                    cos(radians(np.latitude)) * cos(radians(sp.latitude)) *
                    cos(radians(sp.longitude) - radians(np.longitude)) +
                    sin(radians(np.latitude)) * sin(radians(sp.latitude))
                   ), 1
    ) AS distance_miles,
    sp.street_address AS state_park_address,
    sp.city AS state_park_city,
    sp.zip AS state_park_zip,
    sp.longitude AS state_park_longitude,
    sp.latitude AS state_park_latitude,
    sp.function AS state_park_function,
    sp.camping_available,
    sp.boating_available,
    sp.biking_hiking_available,
    sp.fishing_available,
    sp.golf_available,
    sp.equestrian_available,
    sp.ohv_available,
    sp.winter_recreation_available,
    sp.wildlife_available
FROM STAGED.PARKS np
JOIN STAGED.STATE_PARKS sp
ON 3959 * acos(
        cos(radians(np.latitude)) * cos(radians(sp.latitude)) *
        cos(radians(sp.longitude) - radians(np.longitude)) +
        sin(radians(np.latitude)) * sin(radians(sp.latitude))
    ) < 150
ORDER BY np.park_code, distance_miles ASC;