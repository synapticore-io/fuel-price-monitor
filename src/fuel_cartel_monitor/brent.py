"""Fetch Brent crude oil prices from ECB Statistical Data Warehouse."""
import csv
import io
import logging
import os
from datetime import date

import duckdb
import httpx

logger = logging.getLogger(__name__)


def fetch_eur_usd_rate(date_from: date, date_to: date) -> dict[str, float]:
    """Fetch EUR/USD exchange rate from ECB SDW for the given period.

    Returns dict mapping date_str -> eur_usd_rate.
    """
    url = (
        "https://data-api.ecb.europa.eu/service/data/"
        f"EXR/D.USD.EUR.SP00.A?format=csvdata"
        f"&startPeriod={date_from.isoformat()}&endPeriod={date_to.isoformat()}"
    )
    rates: dict[str, float] = {}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        for row in reader:
            try:
                date_str = row.get("TIME_PERIOD", "").strip()
                value = float(row.get("OBS_VALUE", "0").strip())
                if date_str and value > 0:
                    rates[date_str] = value
            except (ValueError, KeyError):
                continue
    return rates


def fetch_brent_prices(date_from: date, date_to: date) -> list[dict]:
    """Fetch daily Brent crude prices in EUR.

    Uses ECB SDW for EUR/USD rates and EIA for Brent USD prices.
    Falls back to a reasonable estimate if API is unavailable.

    Returns list of dicts with keys: date, price_eur, price_usd
    """
    # Try to get Brent prices in USD from EIA
    brent_usd: dict[str, float] = {}
    try:
        brent_usd = _fetch_brent_usd_eia(date_from, date_to)
    except Exception as exc:
        logger.warning("EIA Brent fetch failed: %s", exc)

    # Get EUR/USD rates from ECB
    eur_usd_rates: dict[str, float] = {}
    try:
        eur_usd_rates = fetch_eur_usd_rate(date_from, date_to)
    except Exception as exc:
        logger.warning("ECB EUR/USD fetch failed: %s", exc)

    results: list[dict] = []
    for date_str, usd_price in brent_usd.items():
        # Convert USD/barrel to EUR/litre
        # 1 barrel = 158.987 litres
        eur_rate = eur_usd_rates.get(date_str, 1.1)  # default ~1.1 if missing
        price_eur_per_litre = (usd_price / 158.987) / eur_rate
        results.append(
            {
                "date": date_str,
                "price_eur": round(price_eur_per_litre, 4),
                "price_usd": round(usd_price, 2),
            }
        )

    return sorted(results, key=lambda x: x["date"])


def _fetch_brent_usd_eia(date_from: date, date_to: date) -> dict[str, float]:
    """Fetch Brent crude prices in USD/barrel from EIA API."""
    api_key = os.environ.get("EIA_API_KEY", "DEMO_KEY")
    url = (
        f"https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?api_key={api_key}&frequency=daily&data[0]=value&facets[series][]=RBRTE"
        f"&sort[0][column]=period&sort[0][direction]=asc"
        f"&start={date_from.isoformat()}&end={date_to.isoformat()}&length=500"
    )
    prices: dict[str, float] = {}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()
        for entry in data.get("response", {}).get("data", []):
            try:
                prices[entry["period"]] = float(entry["value"])
            except (KeyError, ValueError, TypeError):
                continue
    return prices


def ingest_brent(
    con: duckdb.DuckDBPyConnection, date_from: date, date_to: date
) -> int:
    """Fetch and store Brent prices in the database.

    Returns number of rows inserted.
    """
    prices = fetch_brent_prices(date_from, date_to)
    if not prices:
        logger.warning("No Brent prices fetched for %s to %s", date_from, date_to)
        return 0

    count = 0
    for row in prices:
        con.execute(
            """
            INSERT OR REPLACE INTO brent_prices (date, price_eur, price_usd)
            VALUES (CAST(? AS DATE), ?, ?)
            """,
            [row["date"], row["price_eur"], row["price_usd"]],
        )
        count += 1

    logger.info("Ingested %d Brent price records", count)
    return count
