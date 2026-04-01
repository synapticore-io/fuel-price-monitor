"""Data ingestion from Tankerkoenig CSV files and live API."""
import logging
import os
import tempfile
from datetime import date, datetime, timedelta

import duckdb
import httpx

logger = logging.getLogger(__name__)

TANKERKOENIG_API_BASE = "https://creativecommons.tankerkoenig.de/json"
TANKERKOENIG_DATA_BASE = (
    "https://data.tankerkoenig.de/tankerkoenig-organization/"
    "tankerkoenig-data/raw/branch/master"
)


def _data_credentials() -> tuple[str, str]:
    """Return (username, password) for Tankerkoenig data server."""
    user = os.environ.get("TANKERKOENIG_DATA_USER", "")
    pw = os.environ.get("TANKERKOENIG_DATA_PASS", "")
    if not user or not pw:
        raise ValueError(
            "TANKERKOENIG_DATA_USER and TANKERKOENIG_DATA_PASS not set. "
            "Check your .env file."
        )
    return user, pw


def download_csv(target_date: date, data_type: str = "prices") -> str:
    """Download a single day's CSV from Tankerkoenig data server.

    Args:
        target_date: The date to download
        data_type: 'prices' or 'stations'

    Returns:
        CSV content as string

    Raises:
        httpx.HTTPStatusError: If the request fails
        ValueError: If credentials are not configured
    """
    user, pw = _data_credentials()

    path = (
        f"/{data_type}/{target_date.year}/{target_date.month:02d}/"
        f"{target_date.isoformat()}-{data_type}.csv"
    )
    url = f"{TANKERKOENIG_DATA_BASE}{path}"

    with httpx.Client(timeout=120.0) as client:
        response = client.get(url, auth=(user, pw), follow_redirects=True)
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
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        row_count = _load_prices_csv(con, tmp_path)
    finally:
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
    count_before = con.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]

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

    count_after = con.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]
    return count_after - count_before


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


def _require_api_key() -> str:
    """Return API key (same as data password) or raise."""
    _, pw = _data_credentials()
    return pw


def ingest_stations_api(
    con: duckdb.DuckDBPyConnection,
    lat: float = 52.37,
    lng: float = 9.73,
    radius_km: float = 25.0,
) -> int:
    """Fetch stations from Tankerkoenig live API and upsert into DB.

    Returns number of stations ingested.
    """
    api_key = _require_api_key()
    url = (
        f"{TANKERKOENIG_API_BASE}/list.php"
        f"?lat={lat}&lng={lng}&rad={radius_km}&sort=dist&type=all&apikey={api_key}"
    )

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()

    if not data.get("ok"):
        raise RuntimeError(f"Tankerkoenig API error: {data.get('message', 'unknown')}")

    stations = data.get("stations", [])
    if not stations:
        logger.warning("No stations found at lat=%s lng=%s rad=%s", lat, lng, radius_km)
        return 0

    count = 0
    for s in stations:
        con.execute(
            """
            INSERT OR REPLACE INTO stations
                (uuid, name, brand, street, house_number, post_code, city, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                s["id"], s.get("name", ""), s.get("brand", ""),
                s.get("street", ""), s.get("houseNumber", ""),
                s.get("postCode", ""), s.get("place", ""),
                float(s["lat"]), float(s["lng"]),
            ],
        )
        count += 1

    logger.info("Ingested %d stations via API (lat=%s lng=%s rad=%skm)", count, lat, lng, radius_km)
    return count


def ingest_prices_api(con: duckdb.DuckDBPyConnection) -> int:
    """Snapshot current prices for all stations in DB via Tankerkoenig API.

    Fetches prices in batches of 10 (API limit).
    Returns number of price records inserted.
    """
    api_key = _require_api_key()

    station_ids = [
        row[0] for row in con.execute("SELECT uuid FROM stations").fetchall()
    ]
    if not station_ids:
        logger.warning("No stations in DB — run ingest_stations_api first")
        return 0

    now = datetime.now()
    count = 0

    # API allows max 10 station IDs per request
    for i in range(0, len(station_ids), 10):
        batch = station_ids[i : i + 10]
        ids_str = ",".join(batch)
        url = f"{TANKERKOENIG_API_BASE}/prices.php?ids={ids_str}&apikey={api_key}"

        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()

        if not data.get("ok"):
            logger.warning("Prices API error for batch %d: %s", i, data.get("message"))
            continue

        for sid, p in data.get("prices", {}).items():
            if p.get("status") != "open":
                continue

            diesel = p.get("diesel")
            e5 = p.get("e5")
            e10 = p.get("e10")

            con.execute(
                """
                INSERT INTO price_changes
                    (timestamp, station_uuid, diesel, e5, e10,
                     diesel_changed, e5_changed, e10_changed)
                VALUES (?, ?, ?, ?, ?, true, true, true)
                """,
                [now, sid, diesel, e5, e10],
            )
            count += 1

    logger.info("Ingested %d price snapshots via API", count)
    return count
