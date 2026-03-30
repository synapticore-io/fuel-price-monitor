"""Fetch Brent crude oil prices from ECB Statistical Data Warehouse."""
import logging
from datetime import date

import duckdb
import httpx

logger = logging.getLogger(__name__)

# ECB Statistical Data Warehouse — Brent crude oil in USD per barrel
# Dataset: FM (Financial Markets), series: D.USD.EUR.SP00.A (EUR/USD rate)
# We use the oil price dataset from ECB SDW:
# https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A
ECB_OIL_URL = (
    "https://data-api.ecb.europa.eu/service/data/"
    "EXR/D.USD.EUR.SP00.A?format=csvdata"
)

# EIA free Brent price API (no key required for recent data)
EIA_BRENT_URL = (
    "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
    "&facets[series][]=RBRTE&sort[0][column]=period&sort[0][direction]=desc&length=365"
)

# Fallback: use stooq.com for Brent data
STOOQ_BRENT_URL = "https://stooq.com/q/d/l/?s=lcou.f&i=d"


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
        lines = response.text.strip().split("\n")
        # Skip header line
        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) < 10:
                continue
            try:
                # DATE column is at index 0, OBS_VALUE at index 7 typically
                # Format: KEY_FAMILY, FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX,
                # TIME_PERIOD, OBS_VALUE, ...
                date_str = parts[6].strip().strip('"')
                value = float(parts[7].strip().strip('"'))
                rates[date_str] = value
            except (ValueError, IndexError):
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
    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        f"?frequency=daily&data[0]=value&facets[series][]=RBRTE"
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
