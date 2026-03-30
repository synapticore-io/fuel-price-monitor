"""Shared test fixtures for fuel-cartel-monitor tests."""
from pathlib import Path

import duckdb
import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with schema initialized."""
    from fuel_cartel_monitor.db import _init_schema

    db = duckdb.connect(":memory:")
    _init_schema(db)
    return db


@pytest.fixture
def con_with_data(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection pre-loaded with sample fixtures."""
    stations_csv = str(FIXTURES_DIR / "sample_stations.csv")
    con.execute(f"""
        INSERT INTO stations (
            uuid, name, brand, street, house_number, post_code, city, latitude, longitude
        )
        SELECT uuid, name, brand, street, house_number, post_code, city,
               CAST(latitude AS DOUBLE), CAST(longitude AS DOUBLE)
        FROM read_csv_auto('{stations_csv}', header=true)
    """)

    prices_csv = str(FIXTURES_DIR / "sample_prices.csv")
    con.execute(f"""
        INSERT INTO price_changes (timestamp, station_uuid, diesel, e5, e10,
                                   diesel_changed, e5_changed, e10_changed)
        SELECT
            CAST("date" AS TIMESTAMP),
            station_uuid,
            CASE WHEN diesel = 0 THEN NULL ELSE diesel END,
            CASE WHEN e5 = 0 THEN NULL ELSE e5 END,
            CASE WHEN e10 = 0 THEN NULL ELSE e10 END,
            CAST(dieselchange AS BOOLEAN),
            CAST(e5change AS BOOLEAN),
            CAST(e10change AS BOOLEAN)
        FROM read_csv_auto('{prices_csv}', header=true)
    """)

    return con
