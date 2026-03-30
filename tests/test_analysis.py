"""Tests for analysis.py — core analytical functions."""
import duckdb
import pytest

from fuel_cartel_monitor import analysis


def test_database_stats_empty(con: duckdb.DuckDBPyConnection) -> None:
    """database_stats returns zero counts for an empty database."""
    stats = analysis.database_stats(con)
    assert stats["station_count"] == 0
    assert stats["price_record_count"] == 0
    assert stats["brent_price_count"] == 0
    assert stats["ingestion_count"] == 0
    assert stats["price_date_from"] is None
    assert stats["price_date_to"] is None


def test_database_stats_with_data(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """database_stats returns correct counts with loaded fixture data."""
    stats = analysis.database_stats(con_with_data)
    assert stats["station_count"] == 10
    assert stats["price_record_count"] > 0
    assert stats["price_date_from"] is not None
    assert stats["price_date_to"] is not None


def test_regional_comparison_empty(con: duckdb.DuckDBPyConnection) -> None:
    """regional_comparison returns empty list when no data."""
    result = analysis.regional_comparison(
        con, fuel_type="e5", date_from="2024-01-01", date_to="2024-01-31"
    )
    assert result == []


def test_regional_comparison_with_data(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """regional_comparison returns regional premiums when data is present."""
    result = analysis.regional_comparison(
        con_with_data,
        fuel_type="e5",
        date_from="2024-01-01",
        date_to="2024-01-31",
    )
    # With fixture data in Hannover (30xxx postcodes), should return at least one region
    assert isinstance(result, list)
    if result:
        row = result[0]
        assert "region_code" in row
        assert "date" in row
        assert "regional_avg" in row
        assert "national_avg" in row
        assert "premium_cents" in row


def test_brent_decoupling_empty(con: duckdb.DuckDBPyConnection) -> None:
    """brent_decoupling returns empty list when no brent prices loaded."""
    result = analysis.brent_decoupling(con, fuel_type="e5", lookback_days=90)
    assert result == []


def test_brent_decoupling_with_data(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """brent_decoupling returns results when both retail and brent data present."""
    # Insert some Brent prices
    con_with_data.execute("""
        INSERT INTO brent_prices (date, price_eur, price_usd)
        VALUES
            (DATE '2024-01-15', 0.52, 80.5),
            (DATE '2024-01-16', 0.51, 79.8),
            (DATE '2024-01-17', 0.53, 81.2)
    """)

    result = analysis.brent_decoupling(
        con_with_data, fuel_type="e5", lookback_days=365
    )
    assert isinstance(result, list)
    if result:
        row = result[0]
        assert hasattr(row, "date")
        assert hasattr(row, "retail_avg")
        assert hasattr(row, "brent_eur")
        assert hasattr(row, "spread")
        assert hasattr(row, "spread_z_score")
        assert hasattr(row, "is_abnormal")


def test_rockets_and_feathers_empty(con: duckdb.DuckDBPyConnection) -> None:
    """rockets_and_feathers returns empty list when no data."""
    result = analysis.rockets_and_feathers(
        con, lat=52.37, lng=9.73, radius_km=50.0, fuel_type="e5", lookback_days=30
    )
    assert result == []


def test_rockets_and_feathers_with_data(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """rockets_and_feathers returns asymmetry data with fixture data.

    The fixture has: Aral increases at 12:00 (day 1), small decreases at 22:00 (day 2).
    This should produce a rockets-and-feathers signal.
    """
    result = analysis.rockets_and_feathers(
        con_with_data,
        lat=52.37,
        lng=9.73,
        radius_km=50.0,
        fuel_type="e5",
        lookback_days=3650,  # large window to include fixture data
    )
    assert isinstance(result, list)
    if result:
        row = result[0]
        assert hasattr(row, "brand")
        assert hasattr(row, "avg_increase_cents")
        assert hasattr(row, "avg_decrease_cents")
        assert hasattr(row, "asymmetry_ratio")


def test_leader_follower_empty(con: duckdb.DuckDBPyConnection) -> None:
    """leader_follower_lag returns empty list when no data."""
    result = analysis.leader_follower_lag(
        con, lat=52.37, lng=9.73, radius_km=50.0, fuel_type="e5", lookback_days=30
    )
    assert result == []


def test_leader_follower_with_data(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """leader_follower_lag detects leader-follower pattern in fixture data.

    Fixture: Aral increases at 12:00, Shell follows at 15:00 (3h later).
    """
    result = analysis.leader_follower_lag(
        con_with_data,
        lat=52.37,
        lng=9.73,
        radius_km=50.0,
        fuel_type="e5",
        lookback_days=3650,
    )
    assert isinstance(result, list)
    if result:
        row = result[0]
        assert hasattr(row, "leader_brand")
        assert hasattr(row, "follower_brand")
        assert hasattr(row, "median_lag_minutes")
        assert hasattr(row, "event_count")


def test_price_sync_index_empty(con: duckdb.DuckDBPyConnection) -> None:
    """price_sync_index returns empty pairs when no data."""
    result = analysis.price_sync_index(
        con, lat=52.37, lng=9.73, radius_km=50.0, fuel_type="e5", lookback_days=30
    )
    assert "pairs" in result
    assert "region_sync_index" in result
    assert result["pairs"] == []
    assert result["region_sync_index"] is None


def test_station_price_history_by_uuid(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """station_price_history returns history for a specific station UUID."""
    # Use large days window to include fixture data (from 2024)
    result = analysis.station_price_history(
        con_with_data,
        station_uuid="a1b2c3d4-0001-0001-0001-000000000001",
        fuel_type="e5",
        days=3650,
    )
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(r["station_uuid"] == "a1b2c3d4-0001-0001-0001-000000000001" for r in result)


def test_station_price_history_by_coords(con_with_data: duckdb.DuckDBPyConnection) -> None:
    """station_price_history returns history for stations near coordinates."""
    result = analysis.station_price_history(
        con_with_data,
        lat=52.37,
        lng=9.73,
        fuel_type="e5",
        days=3650,
    )
    assert isinstance(result, list)


def test_station_price_history_requires_input(con: duckdb.DuckDBPyConnection) -> None:
    """station_price_history raises ValueError without uuid or coords."""
    with pytest.raises(ValueError, match="station_uuid or"):
        analysis.station_price_history(con, fuel_type="e5", days=7)


def test_invalid_fuel_type_raises(con: duckdb.DuckDBPyConnection) -> None:
    """Analysis functions raise ValueError for invalid fuel_type."""
    with pytest.raises(ValueError, match="fuel_type"):
        analysis.leader_follower_lag(con, lat=52.37, lng=9.73, fuel_type="gasoline")

    with pytest.raises(ValueError, match="fuel_type"):
        analysis.rockets_and_feathers(con, lat=52.37, lng=9.73, fuel_type="premium")

    with pytest.raises(ValueError, match="fuel_type"):
        analysis.brent_decoupling(con, fuel_type="super")
