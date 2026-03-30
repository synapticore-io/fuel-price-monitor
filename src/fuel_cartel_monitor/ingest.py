"""Data ingestion from Tankerkoenig CSV files and live API."""
import logging
from datetime import date, timedelta

import duckdb
import httpx

logger = logging.getLogger(__name__)

# Tankerkoenig Azure DevOps raw file URL pattern
AZURE_DEVOPS_BASE = (
    "https://dev.azure.com/tankerkoenig/tankerkoenig-data/"
    "_apis/git/repositories/tankerkoenig-data/items"
)

TANKERKOENIG_API_BASE = "https://creativecommons.tankerkoenig.de/json"


def download_csv(target_date: date, data_type: str = "prices") -> str:
    """Download a single day's CSV from Azure DevOps.

    Args:
        target_date: The date to download
        data_type: 'prices' or 'stations'

    Returns:
        CSV content as string

    Raises:
        httpx.HTTPStatusError: If the request fails
    """
    path = (
        f"/{data_type}/{target_date.year}/{target_date.month:02d}/"
        f"{target_date.isoformat()}-{data_type}.csv"
    )
    url = f"{AZURE_DEVOPS_BASE}?path={path}&api-version=7.0"

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def ingest_day(con: duckdb.DuckDBPyConnection, target_date: date) -> int:
    """Ingest one day of price data into DuckDB.

    Returns number of rows ingested.
    Skips if already ingested (checked via ingestion_log).
    """
    file_path = f"prices/{target_date.isoformat()}"

    # Check if already ingested
    result = con.execute(
        "SELECT row_count FROM ingestion_log WHERE file_path = ?", [file_path]
    ).fetchone()
    if result is not None:
        logger.info("Skipping %s — already ingested (%d rows)", file_path, result[0])
        return result[0]

    logger.info("Downloading prices for %s", target_date.isoformat())
    csv_content = download_csv(target_date, "prices")

    # Write to a temp file for DuckDB to read
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        row_count = _load_prices_csv(con, tmp_path)
    finally:
        import os

        os.unlink(tmp_path)

    # Record in ingestion log
    con.execute(
        "INSERT INTO ingestion_log (file_path, row_count) VALUES (?, ?)",
        [file_path, row_count],
    )
    logger.info("Ingested %d rows for %s", row_count, target_date.isoformat())
    return row_count


def _load_prices_csv(con: duckdb.DuckDBPyConnection, csv_path: str) -> int:
    """Load a prices CSV file into the price_changes table."""
    con.execute(f"""
        INSERT INTO price_changes (
            timestamp, station_uuid, diesel, e5, e10,
            diesel_changed, e5_changed, e10_changed
        )
        SELECT
            CAST("date" AS TIMESTAMP) AS timestamp,
            station_uuid,
            CASE WHEN diesel = 0 THEN NULL ELSE diesel END AS diesel,
            CASE WHEN e5 = 0 THEN NULL ELSE e5 END AS e5,
            CASE WHEN e10 = 0 THEN NULL ELSE e10 END AS e10,
            CAST(dieselchange AS BOOLEAN) AS diesel_changed,
            CAST(e5change AS BOOLEAN) AS e5_changed,
            CAST(e10change AS BOOLEAN) AS e10_changed
        FROM read_csv_auto('{csv_path}', header=true)
        WHERE station_uuid IN (SELECT uuid FROM stations)
    """)

    result = con.execute(
        "SELECT COUNT(*) FROM read_csv_auto(?, header=true)", [csv_path]
    ).fetchone()
    return result[0] if result else 0


def ingest_stations(con: duckdb.DuckDBPyConnection, target_date: date) -> int:
    """Ingest/update station master data."""
    file_path = f"stations/{target_date.isoformat()}"

    # Check if already ingested
    result = con.execute(
        "SELECT row_count FROM ingestion_log WHERE file_path = ?", [file_path]
    ).fetchone()
    if result is not None:
        logger.info("Skipping stations %s — already ingested", file_path)
        return result[0]

    logger.info("Downloading stations for %s", target_date.isoformat())
    csv_content = download_csv(target_date, "stations")

    import os
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        # Upsert stations
        con.execute(f"""
            INSERT OR REPLACE INTO stations (
                uuid, name, brand, street, house_number, post_code, city, latitude, longitude
            )
            SELECT
                uuid,
                name,
                brand,
                street,
                house_number,
                post_code,
                city,
                CAST(latitude AS DOUBLE),
                CAST(longitude AS DOUBLE)
            FROM read_csv_auto('{tmp_path}', header=true)
            WHERE uuid IS NOT NULL
        """)

        result = con.execute(
            "SELECT COUNT(*) FROM read_csv_auto(?, header=true)", [tmp_path]
        ).fetchone()
        row_count = result[0] if result else 0
    finally:
        os.unlink(tmp_path)

    con.execute(
        "INSERT INTO ingestion_log (file_path, row_count) VALUES (?, ?)",
        [file_path, row_count],
    )
    logger.info("Ingested %d stations for %s", row_count, target_date.isoformat())
    return row_count


def ingest_date_range(
    con: duckdb.DuckDBPyConnection,
    date_from: date,
    date_to: date,
) -> dict:
    """Ingest a range of dates. Returns summary stats."""
    days_total = (date_to - date_from).days + 1
    days_ingested = 0
    rows_total = 0
    errors: list[str] = []

    current = date_from
    while current <= date_to:
        try:
            ingest_stations(con, current)
            rows = ingest_day(con, current)
            rows_total += rows
            days_ingested += 1
        except httpx.HTTPStatusError as exc:
            msg = f"{current.isoformat()}: HTTP {exc.response.status_code}"
            logger.warning(msg)
            errors.append(msg)
        except Exception as exc:
            msg = f"{current.isoformat()}: {exc}"
            logger.warning(msg)
            errors.append(msg)

        current += timedelta(days=1)

    return {
        "days_requested": days_total,
        "days_ingested": days_ingested,
        "rows_total": rows_total,
        "errors": errors,
    }


def ingest_latest(con: duckdb.DuckDBPyConnection) -> dict:
    """Ingest the most recent available day (yesterday typically)."""
    yesterday = date.today() - timedelta(days=1)
    return ingest_date_range(con, yesterday, yesterday)
