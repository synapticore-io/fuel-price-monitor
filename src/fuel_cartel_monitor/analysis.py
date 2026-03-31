"""Core analysis functions wrapping DuckDB SQL macros."""
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class LeaderFollowerResult:
    leader_brand: str
    follower_brand: str
    median_lag_minutes: float
    event_count: int


@dataclass
class RocketsFeathersResult:
    brand: str
    avg_increase_cents: float
    avg_decrease_cents: float
    avg_increase_speed_min: float
    avg_decrease_speed_min: float
    asymmetry_ratio: float


@dataclass
class BrentDecouplingResult:
    date: str
    retail_avg: float
    brent_eur: float
    spread: float
    spread_z_score: float
    is_abnormal: bool


def leader_follower_lag(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lng: float,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
    lookback_days: int = 30,
) -> list[LeaderFollowerResult]:
    """Detect leader-follower pricing patterns in a region.

    Args:
        con: DuckDB connection
        lat: Latitude of region center
        lng: Longitude of region center
        radius_km: Search radius in km
        fuel_type: 'diesel', 'e5', or 'e10'
        lookback_days: Number of days to look back

    Returns:
        List of LeaderFollowerResult dataclasses
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT leader_brand, follower_brand, median_lag_minutes, event_count
        FROM leader_follower_lag(?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, lookback_days],
    ).fetchall()

    return [
        LeaderFollowerResult(
            leader_brand=row[0],
            follower_brand=row[1],
            median_lag_minutes=float(row[2]),
            event_count=int(row[3]),
        )
        for row in rows
    ]


def rockets_and_feathers(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lng: float,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
    lookback_days: int = 30,
) -> list[RocketsFeathersResult]:
    """Detect asymmetric price transmission (rockets and feathers).

    Args:
        con: DuckDB connection
        lat: Latitude of region center
        lng: Longitude of region center
        radius_km: Search radius in km
        fuel_type: 'diesel', 'e5', or 'e10'
        lookback_days: Number of days to look back

    Returns:
        List of RocketsFeathersResult dataclasses
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT brand, avg_increase_cents, avg_decrease_cents,
               avg_increase_speed_min, avg_decrease_speed_min, asymmetry_ratio
        FROM rockets_and_feathers(?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, lookback_days],
    ).fetchall()

    return [
        RocketsFeathersResult(
            brand=row[0],
            avg_increase_cents=float(row[1]) if row[1] is not None else 0.0,
            avg_decrease_cents=float(row[2]) if row[2] is not None else 0.0,
            avg_increase_speed_min=float(row[3]) if row[3] is not None else 0.0,
            avg_decrease_speed_min=float(row[4]) if row[4] is not None else 0.0,
            asymmetry_ratio=float(row[5]) if row[5] is not None else 0.0,
        )
        for row in rows
    ]


def price_sync_index(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lng: float,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
    lookback_days: int = 30,
) -> dict:
    """Calculate price synchronization index for a region.

    Args:
        con: DuckDB connection
        lat: Latitude of region center
        lng: Longitude of region center
        radius_km: Search radius in km
        fuel_type: 'diesel', 'e5', or 'e10'
        lookback_days: Number of days to look back

    Returns:
        Dict with keys: pairs (list of dicts), region_sync_index (float)
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT pair_brand_a, pair_brand_b, correlation, is_oligopol_pair, region_sync_index
        FROM price_sync_index(?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, lookback_days],
    ).fetchall()

    pairs = [
        {
            "pair_brand_a": row[0],
            "pair_brand_b": row[1],
            "correlation": float(row[2]) if row[2] is not None else None,
            "is_oligopol_pair": bool(row[3]),
        }
        for row in rows
    ]

    region_sync = float(rows[0][4]) if rows and rows[0][4] is not None else None

    return {
        "pairs": pairs,
        "region_sync_index": region_sync,
    }


def brent_decoupling(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str = "e5",
    lookback_days: int = 90,
) -> list[BrentDecouplingResult]:
    """Track retail-vs-Brent price gap.

    Args:
        con: DuckDB connection
        fuel_type: 'diesel', 'e5', or 'e10'
        lookback_days: Number of days to look back

    Returns:
        List of BrentDecouplingResult dataclasses
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT date, retail_avg, brent_eur, spread, spread_z_score, is_abnormal
        FROM brent_decoupling(?, ?)
        """,
        [fuel_type, lookback_days],
    ).fetchall()

    return [
        BrentDecouplingResult(
            date=str(row[0]),
            retail_avg=float(row[1]),
            brent_eur=float(row[2]),
            spread=float(row[3]),
            spread_z_score=float(row[4]),
            is_abnormal=bool(row[5]),
        )
        for row in rows
    ]


def regional_comparison(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str = "e5",
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Compare fuel prices across regions.

    Args:
        con: DuckDB connection
        fuel_type: 'diesel', 'e5', or 'e10'
        date_from: ISO date string (defaults to 30 days ago)
        date_to: ISO date string (defaults to today)

    Returns:
        List of dicts with region, date, avg price, national avg, premium
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    today = date.today()
    if date_from is None:
        date_from = (today - timedelta(days=30)).isoformat()
    if date_to is None:
        date_to = today.isoformat()

    rows = con.execute(
        """
        SELECT region_code, date, regional_avg, national_avg, premium_cents
        FROM regional_price_comparison(?, ?, ?)
        """,
        [fuel_type, date_from, date_to],
    ).fetchall()

    return [
        {
            "region_code": row[0],
            "date": str(row[1]),
            "regional_avg": float(row[2]),
            "national_avg": float(row[3]),
            "premium_cents": float(row[4]),
        }
        for row in rows
    ]


def station_price_history(
    con: duckdb.DuckDBPyConnection,
    station_uuid: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    fuel_type: str = "e5",
    days: int = 7,
) -> list[dict]:
    """Get price history for a specific station or stations near coordinates.

    Args:
        con: DuckDB connection
        station_uuid: Station UUID (if known)
        lat: Latitude to search near (alternative to station_uuid)
        lng: Longitude to search near (alternative to station_uuid)
        fuel_type: 'diesel', 'e5', or 'e10'
        days: Number of days of history to return

    Returns:
        List of dicts with timestamp, station_uuid, brand, price
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    fuel_column = {"diesel": "diesel", "e5": "e5", "e10": "e10"}[fuel_type]

    if station_uuid is not None:
        rows = con.execute(
            f"""
            SELECT
                pc.timestamp,
                pc.station_uuid,
                s.brand,
                s.name,
                NULLIF(pc.{fuel_column}, 0) AS price
            FROM price_changes pc
            JOIN stations s ON pc.station_uuid = s.uuid
            WHERE pc.station_uuid = ?
              AND pc.timestamp >= CURRENT_TIMESTAMP - INTERVAL (?) DAY
              AND pc.{fuel_column} IS NOT NULL
              AND pc.{fuel_column} > 0
            ORDER BY pc.timestamp DESC
            """,
            [station_uuid, days],
        ).fetchall()
    elif lat is not None and lng is not None:
        rows = con.execute(
            f"""
            SELECT
                pc.timestamp,
                pc.station_uuid,
                s.brand,
                s.name,
                NULLIF(pc.{fuel_column}, 0) AS price
            FROM price_changes pc
            JOIN stations s ON pc.station_uuid = s.uuid
            WHERE 2 * 6371 * ASIN(SQRT(
                POWER(SIN(RADIANS(s.latitude - ?) / 2), 2) +
                COS(RADIANS(?)) * COS(RADIANS(s.latitude)) *
                POWER(SIN(RADIANS(s.longitude - ?) / 2), 2)
            )) <= 5
              AND pc.timestamp >= CURRENT_TIMESTAMP - INTERVAL (?) DAY
              AND pc.{fuel_column} IS NOT NULL
              AND pc.{fuel_column} > 0
            ORDER BY pc.timestamp DESC
            LIMIT 200
            """,
            [lat, lat, lng, days],
        ).fetchall()
    else:
        raise ValueError("Either station_uuid or (lat, lng) must be provided")

    return [
        {
            "timestamp": str(row[0]),
            "station_uuid": row[1],
            "brand": row[2],
            "name": row[3],
            "price": float(row[4]),
        }
        for row in rows
    ]


def database_stats(con: duckdb.DuckDBPyConnection) -> dict:
    """Return database statistics.

    Returns:
        Dict with counts, date ranges, ingestion history
    """
    station_count = con.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    price_count = con.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]
    brent_count = con.execute("SELECT COUNT(*) FROM brent_prices").fetchone()[0]
    ingestion_count = con.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]

    date_range = con.execute(
        "SELECT MIN(timestamp), MAX(timestamp) FROM price_changes"
    ).fetchone()

    recent_ingestions = con.execute(
        """
        SELECT file_path, ingested_at, row_count
        FROM ingestion_log
        ORDER BY ingested_at DESC
        LIMIT 10
        """
    ).fetchall()

    return {
        "station_count": station_count,
        "price_record_count": price_count,
        "brent_price_count": brent_count,
        "ingestion_count": ingestion_count,
        "price_date_from": str(date_range[0]) if date_range[0] else None,
        "price_date_to": str(date_range[1]) if date_range[1] else None,
        "recent_ingestions": [
            {
                "file_path": r[0],
                "ingested_at": str(r[1]),
                "row_count": r[2],
            }
            for r in recent_ingestions
        ],
    }
