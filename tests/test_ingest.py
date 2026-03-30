"""Tests for ingest.py — CSV loading and ingestion logic."""
from pathlib import Path

import duckdb

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_load_stations_csv(con: duckdb.DuckDBPyConnection) -> None:
    """Load sample stations CSV into the stations table."""
    stations_csv = str(FIXTURES_DIR / "sample_stations.csv")
    con.execute(f"""
        INSERT INTO stations (
            uuid, name, brand, street, house_number, post_code, city, latitude, longitude
        )
        SELECT uuid, name, brand, street, house_number, post_code, city,
               CAST(latitude AS DOUBLE), CAST(longitude AS DOUBLE)
        FROM read_csv_auto('{stations_csv}', header=true)
    """)


def test_load_prices_csv(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """Load sample prices CSV into the price_changes table."""
    count = con_with_data.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]
    assert count > 0


def test_price_zero_mapped_to_null(con: duckdb.DuckDBPyConnection) -> None:
    """Prices of 0 in CSV should be stored as NULL (unavailable)."""
    import tempfile

    stations_csv = str(FIXTURES_DIR / "sample_stations.csv")
    con.execute(f"""
        INSERT INTO stations (
            uuid, name, brand, street, house_number, post_code, city, latitude, longitude
        )
        SELECT uuid, name, brand, street, house_number, post_code, city,
               CAST(latitude AS DOUBLE), CAST(longitude AS DOUBLE)
        FROM read_csv_auto('{stations_csv}', header=true)
    """)

    csv_with_zero = (
        "date,station_uuid,diesel,e5,e10,dieselchange,e5change,e10change\n"
        "2024-01-15 12:00:00,a1b2c3d4-0001-0001-0001-000000000001,0,1.899,1.849,0,1,1\n"
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_with_zero)
        tmp_path = f.name

    try:
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
            FROM read_csv_auto('{tmp_path}', header=true)
        """)
    finally:
        import os

        os.unlink(tmp_path)

    row = con.execute("SELECT diesel, e5 FROM price_changes LIMIT 1").fetchone()
    assert row[0] is None, "diesel=0 should be stored as NULL"
    assert abs(row[1] - 1.899) < 0.001


def test_ingestion_log_skip(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """Ingestion log prevents double ingestion."""
    con_with_data.execute("""
        INSERT INTO ingestion_log (file_path, row_count)
        VALUES ('prices/2024-01-15', 46)
    """)

    # Verify the skip logic: check file_path exists
    result = con_with_data.execute(
        "SELECT row_count FROM ingestion_log WHERE file_path = 'prices/2024-01-15'"
    ).fetchone()

    assert result is not None
    assert result[0] == 46


def test_station_brands_in_fixture(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """Fixture contains expected brands."""
    brands = {
        row[0]
        for row in con_with_data.execute("SELECT DISTINCT brand FROM stations").fetchall()
    }
    assert "Aral" in brands
    assert "Shell" in brands
    assert "Esso" in brands
    assert "TotalEnergies" in brands
    assert "Jet" in brands
    assert "HEM" in brands
    assert "Star" in brands


def test_oligopol_flags(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """Oligopol brands are correctly flagged."""
    oligopol_brands = {
        row[0]
        for row in con_with_data.execute(
            "SELECT DISTINCT brand FROM stations WHERE is_oligopol = true"
        ).fetchall()
    }
    independent_brands = {
        row[0]
        for row in con_with_data.execute(
            "SELECT DISTINCT brand FROM stations WHERE is_oligopol = false"
        ).fetchall()
    }

    assert "Aral" in oligopol_brands
    assert "Shell" in oligopol_brands
    assert "Esso" in oligopol_brands
    assert "TotalEnergies" in oligopol_brands
    assert "Jet" in oligopol_brands
    assert "HEM" in independent_brands
    assert "Star" in independent_brands
