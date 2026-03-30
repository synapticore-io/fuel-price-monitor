-- Analytical views for fuel-cartel-monitor

-- Latest price per station
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (station_uuid)
    station_uuid,
    timestamp,
    diesel,
    e5,
    e10
FROM price_changes
ORDER BY station_uuid, timestamp DESC;

-- Station details with latest prices
CREATE OR REPLACE VIEW station_latest_prices AS
SELECT
    s.uuid,
    s.name,
    s.brand,
    s.city,
    s.post_code,
    s.latitude,
    s.longitude,
    s.is_oligopol,
    lp.timestamp AS last_update,
    lp.diesel,
    lp.e5,
    lp.e10
FROM stations s
LEFT JOIN latest_prices lp ON s.uuid = lp.station_uuid;

-- Daily national average prices
CREATE OR REPLACE VIEW daily_national_avg AS
SELECT
    CAST(timestamp AS DATE) AS date,
    AVG(NULLIF(diesel, 0)) AS avg_diesel,
    AVG(NULLIF(e5, 0)) AS avg_e5,
    AVG(NULLIF(e10, 0)) AS avg_e10,
    COUNT(DISTINCT station_uuid) AS station_count
FROM price_changes
GROUP BY CAST(timestamp AS DATE);

-- Daily average prices by brand
CREATE OR REPLACE VIEW daily_brand_avg AS
SELECT
    CAST(pc.timestamp AS DATE) AS date,
    s.brand,
    s.is_oligopol,
    AVG(NULLIF(pc.diesel, 0)) AS avg_diesel,
    AVG(NULLIF(pc.e5, 0)) AS avg_e5,
    AVG(NULLIF(pc.e10, 0)) AS avg_e10,
    COUNT(DISTINCT pc.station_uuid) AS station_count
FROM price_changes pc
JOIN stations s ON pc.station_uuid = s.uuid
GROUP BY CAST(pc.timestamp AS DATE), s.brand, s.is_oligopol;

-- Regional daily averages (by 2-digit post_code prefix)
CREATE OR REPLACE VIEW daily_regional_avg AS
SELECT
    CAST(pc.timestamp AS DATE) AS date,
    LEFT(s.post_code, 2) AS region_code,
    AVG(NULLIF(pc.diesel, 0)) AS avg_diesel,
    AVG(NULLIF(pc.e5, 0)) AS avg_e5,
    AVG(NULLIF(pc.e10, 0)) AS avg_e10,
    COUNT(DISTINCT pc.station_uuid) AS station_count
FROM price_changes pc
JOIN stations s ON pc.station_uuid = s.uuid
WHERE s.post_code IS NOT NULL
GROUP BY CAST(pc.timestamp AS DATE), LEFT(s.post_code, 2);
