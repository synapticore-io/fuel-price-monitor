-- Stations master data
CREATE TABLE IF NOT EXISTS stations (
    uuid VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    brand VARCHAR,
    street VARCHAR,
    house_number VARCHAR,
    post_code VARCHAR,
    city VARCHAR,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    -- Derived: oligopol member flag
    is_oligopol BOOLEAN GENERATED ALWAYS AS (
        brand IN ('ARAL', 'Aral', 'aral',
                  'Shell', 'SHELL', 'shell',
                  'Esso', 'ESSO', 'esso',
                  'TotalEnergies', 'TOTALENERGIES', 'Total', 'TOTAL', 'total',
                  'JET', 'Jet', 'jet')
    ) VIRTUAL
);

-- Price changes (append-only, partitioned by date)
CREATE TABLE IF NOT EXISTS price_changes (
    timestamp TIMESTAMP NOT NULL,
    station_uuid VARCHAR NOT NULL REFERENCES stations(uuid),
    diesel DOUBLE,
    e5 DOUBLE,
    e10 DOUBLE,
    diesel_changed BOOLEAN NOT NULL DEFAULT false,
    e5_changed BOOLEAN NOT NULL DEFAULT false,
    e10_changed BOOLEAN NOT NULL DEFAULT false
);

-- Create index for fast temporal queries
CREATE INDEX IF NOT EXISTS idx_price_changes_time
    ON price_changes (timestamp);
CREATE INDEX IF NOT EXISTS idx_price_changes_station
    ON price_changes (station_uuid);
CREATE INDEX IF NOT EXISTS idx_price_changes_station_time
    ON price_changes (station_uuid, timestamp);

-- Brent crude oil reference prices
CREATE TABLE IF NOT EXISTS brent_prices (
    date DATE PRIMARY KEY,
    price_eur DOUBLE NOT NULL,
    price_usd DOUBLE
);

-- Ingestion log to track what has been loaded
CREATE TABLE IF NOT EXISTS ingestion_log (
    file_path VARCHAR PRIMARY KEY,
    ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    row_count INTEGER NOT NULL
);
