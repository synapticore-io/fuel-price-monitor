"""Tests for db.py — schema initialization and connection management."""
from pathlib import Path

import duckdb


def test_init_schema_creates_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Schema initialization creates all required tables."""
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert "stations" in tables
    assert "price_changes" in tables
    assert "brent_prices" in tables
    assert "ingestion_log" in tables


def test_init_schema_creates_views(con: duckdb.DuckDBPyConnection) -> None:
    """Schema initialization creates analytical views."""
    views = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
        ).fetchall()
    }
    assert "latest_prices" in views
    assert "station_latest_prices" in views
    assert "daily_national_avg" in views
    assert "daily_brand_avg" in views
    assert "daily_regional_avg" in views


def test_stations_is_oligopol_generated_column(con: duckdb.DuckDBPyConnection) -> None:
    """is_oligopol is correctly computed for known brands."""
    con.execute("""
        INSERT INTO stations (
            uuid, name, brand, street, house_number, post_code, city, latitude, longitude
        )
        VALUES
            ('uuid-aral', 'Aral Test', 'Aral', 'Str', '1', '30159', 'Hannover', 52.37, 9.73),
            ('uuid-shell', 'Shell Test', 'Shell', 'Str', '2', '30159', 'Hannover', 52.37, 9.73),
            ('uuid-hem', 'HEM Test', 'HEM', 'Str', '3', '30159', 'Hannover', 52.37, 9.73)
    """)

    rows = con.execute(
        "SELECT brand, is_oligopol FROM stations ORDER BY brand"
    ).fetchall()
    result = {row[0]: row[1] for row in rows}

    assert result["Aral"] is True
    assert result["Shell"] is True
    assert result["HEM"] is False


def test_price_changes_references_stations(con: duckdb.DuckDBPyConnection) -> None:
    """price_changes.station_uuid must reference stations.uuid."""
    con.execute("""
        INSERT INTO stations (
            uuid, name, brand, street, house_number, post_code, city, latitude, longitude
        )
        VALUES ('test-uuid-1', 'Test Station', 'Aral', 'Str', '1', '30159', 'Hannover', 52.37, 9.73)
    """)

    # Valid insert should succeed
    con.execute("""
        INSERT INTO price_changes (timestamp, station_uuid, diesel, e5, e10,
                                   diesel_changed, e5_changed, e10_changed)
        VALUES ('2024-01-15 12:00:00', 'test-uuid-1', 1.799, 1.899, 1.849, true, true, true)
    """)

    count = con.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]
    assert count == 1


def test_ingestion_log_tracks_files(con: duckdb.DuckDBPyConnection) -> None:
    """ingestion_log records file paths and row counts."""
    con.execute("""
        INSERT INTO ingestion_log (file_path, row_count)
        VALUES ('prices/2024-01-15', 1000)
    """)

    row = con.execute(
        "SELECT file_path, row_count FROM ingestion_log"
    ).fetchone()

    assert row[0] == "prices/2024-01-15"
    assert row[1] == 1000


def test_brent_prices_table(con: duckdb.DuckDBPyConnection) -> None:
    """brent_prices stores date and EUR price."""
    con.execute("""
        INSERT INTO brent_prices (date, price_eur, price_usd)
        VALUES (DATE '2024-01-15', 0.52, 80.5)
    """)

    row = con.execute("SELECT price_eur, price_usd FROM brent_prices").fetchone()
    assert abs(row[0] - 0.52) < 0.001
    assert abs(row[1] - 80.5) < 0.001


def test_get_connection_creates_db(tmp_path: Path) -> None:
    """get_connection creates a database at the given path."""
    from fuel_cartel_monitor.db import get_connection

    db_path = tmp_path / "test.duckdb"
    con = get_connection(db_path)
    assert db_path.exists()

    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert "stations" in tables
    con.close()
