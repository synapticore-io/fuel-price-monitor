-- DuckDB SQL macros for oligopoly analysis
-- fuel-cartel-monitor

-- ============================================================
-- MACRO 1: leader_follower_lag
-- Detect time lag between a "leader" brand's price change
-- and "follower" brands matching that change in same region.
-- The Bundeskartellamt documented a 3-hour lag pattern.
-- ============================================================
CREATE OR REPLACE MACRO leader_follower_lag(
    region_lat, region_lng, radius_km, fuel_type, date_from, date_to
) AS TABLE (
    WITH region_stations AS (
        SELECT uuid, brand, latitude, longitude
        FROM stations
        WHERE 2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(latitude - region_lat) / 2), 2) +
            COS(RADIANS(region_lat)) * COS(RADIANS(latitude)) *
            POWER(SIN(RADIANS(longitude - region_lng) / 2), 2)
        )) <= radius_km
        AND is_oligopol = true
    ),
    price_events AS (
        SELECT
            pc.timestamp,
            pc.station_uuid,
            rs.brand,
            CASE
                WHEN fuel_type = 'diesel' THEN pc.diesel
                WHEN fuel_type = 'e5' THEN pc.e5
                WHEN fuel_type = 'e10' THEN pc.e10
            END AS price,
            CASE
                WHEN fuel_type = 'diesel' THEN pc.diesel_changed
                WHEN fuel_type = 'e5' THEN pc.e5_changed
                WHEN fuel_type = 'e10' THEN pc.e10_changed
            END AS price_changed
        FROM price_changes pc
        JOIN region_stations rs ON pc.station_uuid = rs.uuid
        WHERE pc.timestamp >= CAST(date_from AS TIMESTAMP)
          AND pc.timestamp <  CAST(date_to   AS TIMESTAMP)
    ),
    with_prev AS (
        SELECT
            timestamp,
            station_uuid,
            brand,
            price,
            price_changed,
            LAG(price) OVER (
                PARTITION BY station_uuid ORDER BY timestamp
            ) AS prev_price
        FROM price_events
        WHERE price_changed = true AND price > 0
    ),
    increases AS (
        SELECT timestamp, station_uuid, brand, price, prev_price,
               price - prev_price AS delta
        FROM with_prev
        WHERE prev_price IS NOT NULL AND price > prev_price
    ),
    leader_increases AS (
        SELECT timestamp AS leader_time, brand AS leader_brand, delta AS leader_delta
        FROM increases
        WHERE brand IN ('Aral', 'ARAL', 'aral', 'Shell', 'SHELL', 'shell')
    ),
    follower_increases AS (
        SELECT timestamp AS follower_time, brand AS follower_brand, delta AS follower_delta
        FROM increases
        WHERE brand NOT IN ('Aral', 'ARAL', 'aral', 'Shell', 'SHELL', 'shell')
    ),
    matched AS (
        SELECT
            li.leader_brand,
            li.leader_time,
            fi.follower_brand,
            MIN(fi.follower_time) AS first_follower_time
        FROM leader_increases li
        JOIN follower_increases fi
            ON fi.follower_time > li.leader_time
            AND fi.follower_time <= li.leader_time + INTERVAL 24 HOUR
            AND fi.follower_delta > 0
        GROUP BY li.leader_brand, li.leader_time, fi.follower_brand
    )
    SELECT
        leader_brand,
        follower_brand,
        MEDIAN(
            (EPOCH(first_follower_time) - EPOCH(leader_time)) / 60
        ) AS median_lag_minutes,
        COUNT(*) AS event_count
    FROM matched
    GROUP BY leader_brand, follower_brand
    ORDER BY leader_brand, median_lag_minutes
);

-- ============================================================
-- MACRO 2: rockets_and_feathers
-- Detect asymmetric price transmission:
-- Prices rise fast ("rockets") but fall slowly ("feathers").
-- Compare speed of price increases vs decreases.
-- ============================================================
CREATE OR REPLACE MACRO rockets_and_feathers(
    region_lat, region_lng, radius_km, fuel_type, date_from, date_to
) AS TABLE (
    WITH region_stations AS (
        SELECT uuid, brand, latitude, longitude
        FROM stations
        WHERE 2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(latitude - region_lat) / 2), 2) +
            COS(RADIANS(region_lat)) * COS(RADIANS(latitude)) *
            POWER(SIN(RADIANS(longitude - region_lng) / 2), 2)
        )) <= radius_km
        AND is_oligopol = true
    ),
    price_series AS (
        SELECT
            pc.timestamp,
            pc.station_uuid,
            rs.brand,
            CASE
                WHEN fuel_type = 'diesel' THEN pc.diesel
                WHEN fuel_type = 'e5' THEN pc.e5
                WHEN fuel_type = 'e10' THEN pc.e10
            END AS price,
            CASE
                WHEN fuel_type = 'diesel' THEN pc.diesel_changed
                WHEN fuel_type = 'e5' THEN pc.e5_changed
                WHEN fuel_type = 'e10' THEN pc.e10_changed
            END AS price_changed
        FROM price_changes pc
        JOIN region_stations rs ON pc.station_uuid = rs.uuid
        WHERE pc.timestamp >= CAST(date_from AS TIMESTAMP)
          AND pc.timestamp <  CAST(date_to   AS TIMESTAMP)
    ),
    price_deltas AS (
        SELECT
            timestamp,
            station_uuid,
            brand,
            price,
            price_changed,
            LAG(price) OVER (
                PARTITION BY station_uuid ORDER BY timestamp
            ) AS prev_price,
            LAG(timestamp) OVER (
                PARTITION BY station_uuid ORDER BY timestamp
            ) AS prev_timestamp
        FROM price_series
        WHERE price_changed = true AND price > 0
    ),
    classified AS (
        SELECT
            station_uuid,
            brand,
            (price - prev_price) * 100 AS delta_cents,
            EPOCH(timestamp) / 60 - EPOCH(prev_timestamp) / 60 AS minutes_since_prev,
            CASE WHEN price > prev_price THEN 'increase' ELSE 'decrease' END AS direction
        FROM price_deltas
        WHERE prev_price IS NOT NULL AND prev_price > 0
    )
    SELECT
        brand,
        AVG(CASE WHEN direction = 'increase' THEN delta_cents END) AS avg_increase_cents,
        AVG(CASE WHEN direction = 'decrease' THEN ABS(delta_cents) END) AS avg_decrease_cents,
        AVG(CASE WHEN direction = 'increase' THEN minutes_since_prev END) AS avg_increase_speed_min,
        AVG(CASE WHEN direction = 'decrease' THEN minutes_since_prev END) AS avg_decrease_speed_min,
        CASE
            WHEN AVG(CASE WHEN direction = 'increase' THEN minutes_since_prev END) > 0
            THEN AVG(CASE WHEN direction = 'decrease' THEN minutes_since_prev END) /
                 AVG(CASE WHEN direction = 'increase' THEN minutes_since_prev END)
            ELSE NULL
        END AS asymmetry_ratio
    FROM classified
    GROUP BY brand
    ORDER BY brand
);

-- ============================================================
-- MACRO 3: price_sync_index
-- Calculate synchronization index for stations in a region.
-- High synchronization = potential coordinated pricing.
-- ============================================================
CREATE OR REPLACE MACRO price_sync_index(
    region_lat, region_lng, radius_km, fuel_type, date_from, date_to
) AS TABLE (
    WITH region_stations AS (
        SELECT uuid, brand, is_oligopol
        FROM stations
        WHERE 2 * 6371 * ASIN(SQRT(
            POWER(SIN(RADIANS(latitude - region_lat) / 2), 2) +
            COS(RADIANS(region_lat)) * COS(RADIANS(latitude)) *
            POWER(SIN(RADIANS(longitude - region_lng) / 2), 2)
        )) <= radius_km
    ),
    hourly_prices AS (
        SELECT
            DATE_TRUNC('hour', pc.timestamp) AS hour,
            pc.station_uuid,
            rs.brand,
            rs.is_oligopol,
            AVG(
                CASE
                    WHEN fuel_type = 'diesel' THEN NULLIF(pc.diesel, 0)
                    WHEN fuel_type = 'e5' THEN NULLIF(pc.e5, 0)
                    WHEN fuel_type = 'e10' THEN NULLIF(pc.e10, 0)
                END
            ) AS price
        FROM price_changes pc
        JOIN region_stations rs ON pc.station_uuid = rs.uuid
        WHERE pc.timestamp >= CAST(date_from AS TIMESTAMP)
          AND pc.timestamp <  CAST(date_to   AS TIMESTAMP)
        GROUP BY DATE_TRUNC('hour', pc.timestamp), pc.station_uuid, rs.brand, rs.is_oligopol
    ),
    station_pairs AS (
        SELECT
            a.station_uuid AS station_a,
            b.station_uuid AS station_b,
            a.brand AS brand_a,
            b.brand AS brand_b,
            a.is_oligopol AS oligo_a,
            b.is_oligopol AS oligo_b,
            CORR(a.price, b.price) AS correlation
        FROM hourly_prices a
        JOIN hourly_prices b
            ON a.hour = b.hour
            AND a.station_uuid < b.station_uuid
        GROUP BY a.station_uuid, b.station_uuid, a.brand, b.brand, a.is_oligopol, b.is_oligopol
        HAVING COUNT(*) >= 10
    )
    SELECT
        brand_a AS pair_brand_a,
        brand_b AS pair_brand_b,
        AVG(correlation) AS correlation,
        (oligo_a AND oligo_b) AS is_oligopol_pair,
        AVG(AVG(correlation)) OVER () AS region_sync_index
    FROM station_pairs
    GROUP BY brand_a, brand_b, oligo_a, oligo_b
    ORDER BY correlation DESC
);

-- ============================================================
-- MACRO 4: brent_decoupling
-- Track gap between retail fuel prices and Brent crude.
-- Abnormal widening = potential margin extraction.
-- ============================================================
CREATE OR REPLACE MACRO brent_decoupling(
    fuel_type, date_from, date_to
) AS TABLE (
    WITH daily_retail AS (
        SELECT
            CAST(timestamp AS DATE) AS date,
            AVG(
                CASE
                    WHEN fuel_type = 'diesel' THEN NULLIF(diesel, 0)
                    WHEN fuel_type = 'e5' THEN NULLIF(e5, 0)
                    WHEN fuel_type = 'e10' THEN NULLIF(e10, 0)
                END
            ) AS retail_avg
        FROM price_changes
        WHERE CAST(timestamp AS DATE) >= CAST(date_from AS DATE)
          AND CAST(timestamp AS DATE) <  CAST(date_to   AS DATE)
        GROUP BY CAST(timestamp AS DATE)
    ),
    -- ASOF LEFT JOIN: fuer jeden Retail-Tag den letzten verfuegbaren
    -- Brent-Wert nehmen (Wochenenden, Feiertage, API-Luecken werden mit
    -- dem letzten Spot fortgeschrieben). Marktueblich.
    joined AS (
        SELECT
            dr.date,
            dr.retail_avg,
            bp.price_eur AS brent_eur,
            dr.retail_avg - bp.price_eur AS spread
        FROM daily_retail dr
        ASOF LEFT JOIN brent_prices bp ON dr.date >= bp.date
        WHERE dr.retail_avg IS NOT NULL
          AND bp.price_eur IS NOT NULL
    ),
    with_stats AS (
        SELECT
            date,
            retail_avg,
            brent_eur,
            spread,
            AVG(spread) OVER (
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS rolling_mean,
            STDDEV(spread) OVER (
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS rolling_std
        FROM joined
    )
    SELECT
        date,
        retail_avg,
        brent_eur,
        spread,
        CASE
            WHEN rolling_std > 0
            THEN (spread - rolling_mean) / rolling_std
            ELSE 0
        END AS spread_z_score,
        CASE
            WHEN rolling_std > 0
            THEN (spread - rolling_mean) / rolling_std > 2.0
            ELSE false
        END AS is_abnormal
    FROM with_stats
    ORDER BY date
);

-- ============================================================
-- MACRO 5: regional_price_comparison
-- Compare fuel prices across regions and against national avg.
-- ============================================================
CREATE OR REPLACE MACRO regional_price_comparison(
    fuel_type, date_from, date_to
) AS TABLE (
    WITH regional AS (
        SELECT
            CAST(pc.timestamp AS DATE) AS date,
            LEFT(s.post_code, 2) AS region_code,
            AVG(
                CASE
                    WHEN fuel_type = 'diesel' THEN NULLIF(pc.diesel, 0)
                    WHEN fuel_type = 'e5' THEN NULLIF(pc.e5, 0)
                    WHEN fuel_type = 'e10' THEN NULLIF(pc.e10, 0)
                END
            ) AS regional_avg
        FROM price_changes pc
        JOIN stations s ON pc.station_uuid = s.uuid
        WHERE CAST(pc.timestamp AS DATE) >= CAST(date_from AS DATE)
          AND CAST(pc.timestamp AS DATE) <= CAST(date_to AS DATE)
          AND s.post_code IS NOT NULL
        GROUP BY CAST(pc.timestamp AS DATE), LEFT(s.post_code, 2)
    ),
    national AS (
        SELECT
            CAST(pc.timestamp AS DATE) AS date,
            AVG(
                CASE
                    WHEN fuel_type = 'diesel' THEN NULLIF(pc.diesel, 0)
                    WHEN fuel_type = 'e5' THEN NULLIF(pc.e5, 0)
                    WHEN fuel_type = 'e10' THEN NULLIF(pc.e10, 0)
                END
            ) AS national_avg
        FROM price_changes pc
        JOIN stations s ON pc.station_uuid = s.uuid
        WHERE CAST(pc.timestamp AS DATE) >= CAST(date_from AS DATE)
          AND CAST(pc.timestamp AS DATE) <= CAST(date_to AS DATE)
        GROUP BY CAST(pc.timestamp AS DATE)
    )
    SELECT
        r.region_code,
        r.date,
        r.regional_avg,
        n.national_avg,
        (r.regional_avg - n.national_avg) * 100 AS premium_cents
    FROM regional r
    JOIN national n ON r.date = n.date
    WHERE r.regional_avg IS NOT NULL AND n.national_avg IS NOT NULL
    ORDER BY r.date, r.region_code
);
