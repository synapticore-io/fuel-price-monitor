"""Core analysis functions wrapping DuckDB SQL macros."""
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Steuer- und Emissions-Konstanten (Deutschland, Stand 2026)
# ---------------------------------------------------------------------------
# Energiesteuer: fest per Gesetz (EnergieStG §2) — EUR pro Liter, netto.
# Tankrabatt 2026: -14,04 ct/L auf Diesel UND Benzin vom 1.5. bis 30.6.2026
# (Bundestag-Beschluss 24.4.2026, Union+SPD).
# Quellen:
#   https://www.bundestag.de/dokumente/textarchiv/2026/kw17-de-energiesteuersenkung-1165890
#   https://www.gesetze-im-internet.de/energiestg/__2.html
ENERGY_TAX_DIESEL_NORMAL_EUR = 0.4704
ENERGY_TAX_DIESEL_RABATT_EUR = 0.3300  # 47,04 - 14,04
ENERGY_TAX_E5_NORMAL_EUR = 0.6545
ENERGY_TAX_E5_RABATT_EUR = 0.5141  # 65,45 - 14,04
ENERGY_TAX_E10_NORMAL_EUR = 0.6545
ENERGY_TAX_E10_RABATT_EUR = 0.5141

# Tankrabatt-Perioden als half-open Intervalle [start, end)
TANKRABATT_PERIODS = [
    (date(2026, 5, 1), date(2026, 7, 1)),  # Mai + Juni 2026
]

# Backwards-compat / Default-Aliase fuer den Normalsatz
ENERGY_TAX_DIESEL_EUR = ENERGY_TAX_DIESEL_NORMAL_EUR
ENERGY_TAX_E5_EUR = ENERGY_TAX_E5_NORMAL_EUR
ENERGY_TAX_E10_EUR = ENERGY_TAX_E10_NORMAL_EUR


def _to_date(x) -> date:
    """Coerce date-or-isoformat-string to a date instance."""
    return x if isinstance(x, date) else date.fromisoformat(str(x))


def _energy_tax_for_period(fuel_type: str, date_from, date_to) -> tuple[float, str]:
    """Days-weighted average energy tax (EUR/L) for [date_from, date_to).

    Accounts for Tankrabatt overlaps. Returns (rate_eur, source_string).
    Periods fully inside or outside a Tankrabatt window return the exact
    rate; partial overlaps return the tagesgewichtete Mittelung.
    """
    df = _to_date(date_from)
    dt = _to_date(date_to)
    normal = {
        "diesel": ENERGY_TAX_DIESEL_NORMAL_EUR,
        "e5": ENERGY_TAX_E5_NORMAL_EUR,
        "e10": ENERGY_TAX_E10_NORMAL_EUR,
    }[fuel_type]
    rabatt = {
        "diesel": ENERGY_TAX_DIESEL_RABATT_EUR,
        "e5": ENERGY_TAX_E5_RABATT_EUR,
        "e10": ENERGY_TAX_E10_RABATT_EUR,
    }[fuel_type]
    total_days = (dt - df).days
    rabatt_days = 0
    for r_start, r_end in TANKRABATT_PERIODS:
        overlap_start = max(df, r_start)
        overlap_end = min(dt, r_end)
        if overlap_start < overlap_end:
            rabatt_days += (overlap_end - overlap_start).days
    if total_days <= 0:
        return normal, "EnergieStG §2 (Normalsatz)"
    rate = (rabatt_days * rabatt + (total_days - rabatt_days) * normal) / total_days
    if rabatt_days == 0:
        src = "EnergieStG §2 (Diesel 47,04 ct/L · Super 65,45 ct/L)"
    elif rabatt_days == total_days:
        src = "EnergieStG §2 mit Tankrabatt 1.5.–30.6.2026 (-14,04 ct/L auf Diesel und Benzin)"
    else:
        pct = round(rabatt_days / total_days * 100)
        src = f"EnergieStG §2 — {pct} % der Periode mit Tankrabatt (-14,04 ct/L)"
    return rate, src

# CO2-Preis nach BEHG § 10 — 2026 Preiskorridor 55–65 €/t, Mindestpreis 55
# Erste EEX-Auktion: 1. Juli 2026. Vor diesem Datum gibt es keinen realisierten
# Marktpreis, daher Mindestpreis als konservativer Ansatz.
# Quelle: https://www.gesetze-im-internet.de/behg/__10.html (BEHG §10)
#         https://www.dehst.de/SharedDocs/news/DE/behv-nehs-ets2-aenderung-verabschiedet.html
CO2_PRICE_EUR_PER_TON = 55.0
CO2_PRICE_SOURCE = "BEHG §10, Mindestpreis Korridor 2026 (55–65 €/t); erste EEX-Auktion ab 1.7.2026"

# Emissions-Standardwerte laut UBA/BAFA — kg CO2 pro Liter
CO2_KG_PER_LITER_DIESEL = 2.64
CO2_KG_PER_LITER_E5 = 2.32
CO2_KG_PER_LITER_E10 = 2.23

# Mehrwertsteuer auf den Brutto-Verkaufspreis (alle Komponenten)
VAT_RATE = 0.19


def _month_bounds(month: str) -> tuple[date, date]:
    """Return (first_day_of_month, first_day_of_next_month) for 'YYYY-MM'."""
    year, mon = month.split("-")
    start = date(int(year), int(mon), 1)
    end = date(int(year) + 1, 1, 1) if int(mon) == 12 else date(int(year), int(mon) + 1, 1)
    return start, end


def _resolve_range(
    date_from: date | str | None,
    date_to: date | str | None,
    lookback_days: int | None,
) -> tuple[date, date]:
    """Resolve either explicit range or lookback-based range to (from, to_exclusive)."""
    if date_from is not None and date_to is not None:
        df = date.fromisoformat(date_from) if isinstance(date_from, str) else date_from
        dt = date.fromisoformat(date_to) if isinstance(date_to, str) else date_to
        return df, dt
    days = lookback_days if lookback_days is not None else 30
    today = date.today()
    return today - timedelta(days=days), today + timedelta(days=1)


def _date_range_where(
    ts_column: str,
    date_from: date | str | None,
    date_to: date | str | None,
) -> tuple[str, list]:
    """Build a WHERE clause (or empty string) for a half-open timestamp range."""
    if date_from is None or date_to is None:
        return "", []
    return (
        f"WHERE CAST({ts_column} AS DATE) >= CAST(? AS DATE) "
        f"  AND CAST({ts_column} AS DATE) <  CAST(? AS DATE)",
        [str(date_from), str(date_to)],
    )


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
    date_from: date | str,
    date_to: date | str,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
) -> list[LeaderFollowerResult]:
    """Detect leader-follower pricing patterns in a region.

    date_from inclusive, date_to exclusive — matches the macro's half-open window.
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT leader_brand, follower_brand, median_lag_minutes, event_count
        FROM leader_follower_lag(?, ?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, str(date_from), str(date_to)],
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
    date_from: date | str,
    date_to: date | str,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
) -> list[RocketsFeathersResult]:
    """Detect asymmetric price transmission (rockets and feathers) over [date_from, date_to)."""
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT brand, avg_increase_cents, avg_decrease_cents,
               avg_increase_speed_min, avg_decrease_speed_min, asymmetry_ratio
        FROM rockets_and_feathers(?, ?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, str(date_from), str(date_to)],
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
    date_from: date | str,
    date_to: date | str,
    radius_km: float = 25.0,
    fuel_type: str = "e5",
) -> dict:
    """Calculate price synchronization index for a region over [date_from, date_to)."""
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT pair_brand_a, pair_brand_b, correlation, is_oligopol_pair, region_sync_index
        FROM price_sync_index(?, ?, ?, ?, ?, ?)
        """,
        [lat, lng, radius_km, fuel_type, str(date_from), str(date_to)],
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
    date_from: date | str,
    date_to: date | str,
    fuel_type: str = "e5",
) -> list[BrentDecouplingResult]:
    """Track retail-vs-Brent price gap over [date_from, date_to)."""
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    rows = con.execute(
        """
        SELECT date, retail_avg, brent_eur, spread, spread_z_score, is_abnormal
        FROM brent_decoupling(?, ?, ?)
        """,
        [fuel_type, str(date_from), str(date_to)],
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



def best_time_to_tank(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str = "e5",
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> dict:
    """Analyze best time to tank by hour of day and day of week.

    If date_from/date_to given, restrict to [date_from, date_to). Else full table.
    """
    fuel_column = {"diesel": "diesel", "e5": "e5", "e10": "e10"}[fuel_type]
    where, params = _date_range_where("timestamp", date_from, date_to)

    by_hour = con.execute(f"""
        SELECT
            EXTRACT(HOUR FROM timestamp) AS hour,
            ROUND(AVG(NULLIF({fuel_column}, 0)), 4) AS avg_price
        FROM price_changes
        {where}
        GROUP BY hour ORDER BY hour
    """, params).fetchall()

    by_dow = con.execute(f"""
        SELECT
            EXTRACT(DOW FROM timestamp) AS dow,
            ROUND(AVG(NULLIF({fuel_column}, 0)), 4) AS avg_price
        FROM price_changes
        {where}
        GROUP BY dow ORDER BY dow
    """, params).fetchall()

    dow_names = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"]

    hours = [{"hour": int(r[0]), "avg_price": float(r[1])} for r in by_hour]
    weekdays = [
        {"day": dow_names[int(r[0])], "dow": int(r[0]), "avg_price": float(r[1])}
        for r in by_dow
    ]

    cheapest_hour = min(hours, key=lambda x: x["avg_price"])
    priciest_hour = max(hours, key=lambda x: x["avg_price"])
    cheapest_day = min(weekdays, key=lambda x: x["avg_price"])
    priciest_day = max(weekdays, key=lambda x: x["avg_price"])

    return {
        "by_hour": hours,
        "by_weekday": weekdays,
        "cheapest_hour": cheapest_hour["hour"],
        "priciest_hour": priciest_hour["hour"],
        "hour_spread_cents": round(
            (priciest_hour["avg_price"] - cheapest_hour["avg_price"]) * 100, 1
        ),
        "cheapest_day": cheapest_day["day"],
        "priciest_day": priciest_day["day"],
        "day_spread_cents": round(
            (priciest_day["avg_price"] - cheapest_day["avg_price"]) * 100, 1
        ),
    }


def brand_ranking(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str = "e5",
    min_stations: int = 50,
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> list[dict]:
    """Rank brands by average fuel price over optional [date_from, date_to)."""
    fuel_column = {"diesel": "diesel", "e5": "e5", "e10": "e10"}[fuel_type]
    ts_filter, ts_params = _date_range_where("pc.timestamp", date_from, date_to)
    # ts_filter starts with WHERE; splice into existing WHERE chain
    extra_and = ts_filter.replace("WHERE ", "AND ", 1) if ts_filter else ""

    rows = con.execute(f"""
        SELECT
            s.brand,
            s.is_oligopol,
            ROUND(AVG(NULLIF(pc.{fuel_column}, 0)), 4) AS avg_price,
            COUNT(DISTINCT pc.station_uuid) AS station_count
        FROM price_changes pc
        JOIN stations s ON pc.station_uuid = s.uuid
        WHERE s.brand IS NOT NULL AND s.brand != ''
          {extra_and}
        GROUP BY s.brand, s.is_oligopol
        HAVING COUNT(DISTINCT pc.station_uuid) >= ?
        ORDER BY avg_price
    """, [*ts_params, min_stations]).fetchall()

    return [
        {
            "brand": r[0],
            "is_oligopol": bool(r[1]),
            "avg_price": float(r[2]),
            "station_count": int(r[3]),
        }
        for r in rows
    ]


def _q2(x) -> Decimal:
    """Quantize to 2 decimal places EUR (= ganze Cent), ROUND_HALF_UP."""
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def price_breakdown(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str,
    date_from: date | str,
    date_to: date | str,
) -> dict:
    """Decompose the average retail price of a fuel into its cost components.

    All values returned as Decimal-rounded floats (2 decimal places EUR = ganze Cent)
    using ROUND_HALF_UP. The identity Σ components = retail holds *exactly* after
    rounding because residual is computed from the rounded retail minus rounded
    others — not from the raw float arithmetic. Anzeige damit Mensch-rechenbar:
    0,47 + 0,15 + 0,56 + 0,36 + 0,73 = 2,27 €.

    Components (per litre, EUR):
      - energy_tax     — Energiesteuer (fix by EnergieStG)
      - co2            — CO2-Abgabe = CO2_kg_per_litre × CO2_price / 1000
      - brent          — monthly mean Brent spot price in EUR/litre
      - vat            — 19 % anteilig am Bruttopreis = retail × 0,19 / 1,19
      - residual       — retail − (energy_tax + co2 + brent + vat) [Raffinerie + Marge]
    """
    if fuel_type not in ("diesel", "e5", "e10"):
        raise ValueError(f"fuel_type must be one of diesel/e5/e10, got: {fuel_type!r}")

    fuel_column = {"diesel": "diesel", "e5": "e5", "e10": "e10"}[fuel_type]
    energy_tax_raw, energy_tax_source = _energy_tax_for_period(
        fuel_type, date_from, date_to
    )
    co2_kg = {
        "diesel": CO2_KG_PER_LITER_DIESEL,
        "e5": CO2_KG_PER_LITER_E5,
        "e10": CO2_KG_PER_LITER_E10,
    }[fuel_type]

    retail_avg = con.execute(
        f"""
        SELECT AVG(NULLIF({fuel_column}, 0))
        FROM price_changes
        WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE)
          AND CAST(timestamp AS DATE) <  CAST(? AS DATE)
        """,
        [str(date_from), str(date_to)],
    ).fetchone()[0]

    brent_avg = con.execute(
        """
        SELECT AVG(price_eur)
        FROM brent_prices
        WHERE date >= CAST(? AS DATE) AND date < CAST(? AS DATE)
        """,
        [str(date_from), str(date_to)],
    ).fetchone()[0]

    if retail_avg is None or brent_avg is None:
        return {}

    # Decimal-Arithmetik durchgaengig — IEEE-Float-Drift vermeiden
    retail = _q2(retail_avg)
    energy_tax = _q2(energy_tax_raw)
    co2 = _q2(Decimal(str(co2_kg)) * Decimal(str(CO2_PRICE_EUR_PER_TON)) / Decimal("1000"))
    brent = _q2(brent_avg)
    vat_rate = Decimal(str(VAT_RATE))
    vat = _q2(retail * vat_rate / (Decimal("1") + vat_rate))
    # Residuum aus den GERUNDETEN Komponenten — Identitaet schliesst exakt
    residual = _q2(retail - energy_tax - co2 - brent - vat)

    return {
        "retail_avg_eur": float(retail),
        "energy_tax_eur": float(energy_tax),
        "co2_eur": float(co2),
        "brent_eur": float(brent),
        "vat_eur": float(vat),
        "residual_eur": float(residual),
        "co2_price_eur_per_ton": CO2_PRICE_EUR_PER_TON,
        "co2_kg_per_liter": co2_kg,
        "co2_price_source": CO2_PRICE_SOURCE,
        "energy_tax_source": energy_tax_source,
    }


def consumer_impact(
    con: duckdb.DuckDBPyConnection,
    fuel_type: str = "e5",
    tank_size_liters: int = 50,
    tanks_per_year: int = 52,
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> dict:
    """Calculate the cost impact of oligopol pricing on consumers."""
    fuel_column = {"diesel": "diesel", "e5": "e5", "e10": "e10"}[fuel_type]
    where, params = _date_range_where("pc.timestamp", date_from, date_to)

    row = con.execute(f"""
        SELECT
            ROUND(AVG(CASE WHEN s.is_oligopol THEN
                NULLIF(pc.{fuel_column}, 0) END), 4) AS oligo_avg,
            ROUND(AVG(CASE WHEN NOT s.is_oligopol THEN
                NULLIF(pc.{fuel_column}, 0) END), 4) AS indie_avg
        FROM price_changes pc
        JOIN stations s ON pc.station_uuid = s.uuid
        {where}
    """, params).fetchone()

    oligo_avg = float(row[0]) if row[0] else 0
    indie_avg = float(row[1]) if row[1] else 0
    premium_eur = oligo_avg - indie_avg
    premium_cents = round(premium_eur * 100, 2)
    per_tank = round(premium_eur * tank_size_liters, 2)
    per_year = round(per_tank * tanks_per_year, 2)

    return {
        "oligopol_avg": oligo_avg,
        "independent_avg": indie_avg,
        "premium_cents_per_liter": premium_cents,
        "premium_per_tank_eur": per_tank,
        "premium_per_year_eur": per_year,
        "tank_size_liters": tank_size_liters,
        "tanks_per_year": tanks_per_year,
    }
