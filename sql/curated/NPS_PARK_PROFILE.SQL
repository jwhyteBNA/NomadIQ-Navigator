CREATE OR REPLACE TABLE CURATED.NPS_PARK_PROFILE AS
SELECT
    p.park_code,
    p.name,
    p.states,
    p.latitude,
    p.longitude,
    p.designation,
    p.description,
    a.line1 AS address_line1,
    a.line2 AS address_line2,
    a.city AS address_city,
    a.state AS address_state,
    a.postal AS address_zip,
    e.email_address AS email,
    ph.phone_number,
    ph.extension AS phone_extension,
    ef.entrance_fees,
    ep.cost AS annual_pass_fee,
    ep.description AS annual_pass_description,
    act.activities,
    GROUP_CONCAT(t.topic_name, ', ' ORDER BY t.topic_name) AS park_themes
FROM STAGED.PARKS p
LEFT JOIN STAGED.ADDRESSES a
    ON p.id = a.park_id AND a.address_type = 'Physical'
LEFT JOIN STAGED.EMAILS e ON p.id = e.park_id
LEFT JOIN STAGED.PHONES ph ON p.id = ph.park_id AND ph.phone_type = 'Voice'
LEFT JOIN (
    SELECT
        park_id,
        GROUP_CONCAT(title || ' ($' || cost::VARCHAR || ')', ', ') AS entrance_fees
    FROM STAGED.ENTRANCE_FEES
    WHERE cost <> 0.00
    GROUP BY park_id
) ef ON p.id = ef.park_id
LEFT JOIN STAGED.ENTRANCE_PASSES ep ON p.id = ep.park_id
LEFT JOIN STAGED.TOPICS t ON p.id = t.park_id
LEFT JOIN (
    SELECT
        park_id,
        GROUP_CONCAT(activity_name, ', ' ORDER BY activity_name) AS activities
    FROM STAGED.ACTIVITIES
    GROUP BY park_id
) act ON p.id = act.park_id
GROUP BY
    p.park_code, p.name, p.states, p.latitude, p.longitude, p.designation, p.description,
    a.line1, a.line2, a.city, a.state, a.postal,
    e.email_address,
    ph.phone_number, ph.extension, ef.entrance_fees,
    ep.cost, ep.description, act.activities
ORDER BY p.name;