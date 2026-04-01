"""MCP server exposing fuel price analysis tools."""
import json
import logging
from datetime import date

import duckdb
from mcp.server.fastmcp import FastMCP
from mcp_ui_server import create_ui_resource

from fuel_cartel_monitor import analysis
from fuel_cartel_monitor.db import get_connection
from fuel_cartel_monitor.ingest import (
    ingest_date_range,
    ingest_latest,
    ingest_prices_api,
    ingest_stations_api,
)

logger = logging.getLogger(__name__)

mcp = FastMCP("fuel-cartel-monitor")

_con: duckdb.DuckDBPyConnection | None = None


def _get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = get_connection()
    return _con


def _json(obj: object) -> str:
    return json.dumps(obj, indent=2, default=str)


def _ui(uri_path: str, html: str) -> object:
    """Create a UI resource for rendering in Claude Desktop."""
    return create_ui_resource({
        "uri": f"ui://fuel-cartel-monitor/{uri_path}",
        "content": {"type": "rawHtml", "htmlString": html},
        "encoding": "text",
    })


def _chart_html(title: str, chart_config: str) -> str:
    """Generate standalone HTML page with Chart.js visualization."""
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 16px; background: #0d1117; color: #c9d1d9; }}
  h2 {{ color: #58a6ff; margin-bottom: 8px; font-size: 16px; }}
  canvas {{ max-height: 400px; }}
</style>
</head><body>
<h2>{title}</h2>
<canvas id="chart"></canvas>
<script>
new Chart(document.getElementById('chart'), {chart_config});
</script>
</body></html>"""


@mcp.tool()
def analyze_leader_follower(
    lat: float, lng: float, radius_km: float = 25.0,
    fuel_type: str = "e5", lookback_days: int = 30,
) -> list:
    """Detect leader-follower pricing patterns between major oil brands.

    Shows which brand initiates price changes and how quickly others follow.
    The Bundeskartellamt documented a typical 3-hour lag between Aral/Shell and followers.
    """
    results = analysis.leader_follower_lag(
        _get_con(), lat, lng, radius_km, fuel_type, lookback_days
    )
    data = [vars(r) for r in results]

    if not data:
        return [_json(data)]

    labels = json.dumps([f"{d['leader_brand']}→{d['follower_brand']}" for d in data])
    lags = json.dumps([d["median_lag_minutes"] for d in data])
    counts = json.dumps([d["event_count"] for d in data])

    chart = _chart_html("Leader→Follower Lag (minutes)", f"""{{
        type: 'bar',
        data: {{
            labels: {labels},
            datasets: [{{
                label: 'Median Lag (min)',
                data: {lags},
                backgroundColor: 'rgba(88,166,255,0.6)',
                borderColor: '#58a6ff', borderWidth: 1
            }}]
        }},
        options: {{
            indexAxis: 'y',
            plugins: {{ subtitle: {{ display: true, text: 'Events: ' + {counts}.join(', ') }} }}
        }}
    }}""")

    return [_json(data), _ui("leader-follower", chart)]


@mcp.tool()
def analyze_rockets_feathers(
    lat: float, lng: float, radius_km: float = 25.0,
    fuel_type: str = "e5", lookback_days: int = 30,
) -> list:
    """Detect asymmetric price transmission: prices rise fast (rockets) but fall slowly (feathers).

    An asymmetry ratio > 1.0 indicates the pattern.
    """
    results = analysis.rockets_and_feathers(
        _get_con(), lat, lng, radius_km, fuel_type, lookback_days
    )
    data = [vars(r) for r in results]

    if not data:
        return [_json(data)]

    labels = json.dumps([d["brand"] for d in data])
    ups = json.dumps([round(d["avg_increase_cents"], 1) for d in data])
    downs = json.dumps([round(d["avg_decrease_cents"], 1) for d in data])

    chart = _chart_html("Rockets & Feathers — Avg Price Change (cents)", f"""{{
        type: 'bar',
        data: {{
            labels: {labels},
            datasets: [
                {{ label: '↑ Increase', data: {ups}, backgroundColor: 'rgba(255,99,71,0.7)' }},
                {{ label: '↓ Decrease', data: {downs}, backgroundColor: 'rgba(50,205,50,0.7)' }}
            ]
        }}
    }}""")

    return [_json(data), _ui("rockets-feathers", chart)]


@mcp.tool()
def analyze_brent_decoupling(
    fuel_type: str = "diesel", lookback_days: int = 90,
) -> list:
    """Track the gap between retail fuel prices and Brent crude oil price.

    Flags abnormal widening that may indicate margin extraction.
    """
    results = analysis.brent_decoupling(_get_con(), fuel_type, lookback_days)
    data = [vars(r) for r in results]

    if not data:
        return [_json(data)]

    dates = json.dumps([d["date"] for d in data])
    spreads = json.dumps([round(d["spread"], 4) for d in data])
    z_scores = json.dumps([round(d["spread_z_score"], 2) for d in data])

    chart = _chart_html("Brent Decoupling — Retail vs Crude Spread", f"""{{
        type: 'line',
        data: {{
            labels: {dates},
            datasets: [
                {{ label: 'Spread (EUR)', data: {spreads},
                   borderColor: '#58a6ff', fill: false, tension: 0.2 }},
                {{ label: 'Z-Score', data: {z_scores},
                   borderColor: '#ff6347', fill: false,
                   tension: 0.2, yAxisID: 'y2' }}
            ]
        }},
        options: {{
            scales: {{
                y: {{ title: {{ display: true, text: 'Spread' }} }},
                y2: {{ position: 'right',
                       title: {{ display: true, text: 'Z-Score' }},
                       grid: {{ drawOnChartArea: false }} }}
            }}
        }}
    }}""")

    return [_json(data), _ui("brent-decoupling", chart)]


@mcp.tool()
def analyze_price_sync(
    lat: float, lng: float, radius_km: float = 25.0,
    fuel_type: str = "e5", lookback_days: int = 30,
) -> str:
    """Calculate price synchronization index for fuel stations in a region.

    High sync between oligopol members vs low sync with independents suggests coordination.
    """
    return _json(analysis.price_sync_index(
        _get_con(), lat, lng, radius_km, fuel_type, lookback_days
    ))


@mcp.tool()
def compare_regions(
    fuel_type: str = "e5", date_from: str | None = None, date_to: str | None = None,
) -> str:
    """Compare fuel prices across German regions by postal code prefix."""
    return _json(analysis.regional_comparison(_get_con(), fuel_type, date_from, date_to))


@mcp.tool()
def station_price_history(
    station_uuid: str | None = None, lat: float | None = None, lng: float | None = None,
    fuel_type: str = "e5", days: int = 7,
) -> str:
    """Get price history for a specific fuel station by UUID or near coordinates."""
    return _json(analysis.station_price_history(
        _get_con(), station_uuid, lat, lng, fuel_type, days
    ))


@mcp.tool()
def ingest_data(
    date_from: str | None = None, date_to: str | None = None,
    latest: bool = False, api_stations: bool = False, api_prices: bool = False,
    lat: float = 52.37, lng: float = 9.73, radius_km: float = 25.0,
) -> str:
    """Ingest Tankerkoenig data.

    Use api_stations/api_prices for live API,
    or latest/date range for CSV bulk ingestion.
    """
    con = _get_con()
    if api_stations:
        return _json({"stations_ingested": ingest_stations_api(con, lat, lng, radius_km)})
    if api_prices:
        return _json({"prices_ingested": ingest_prices_api(con)})
    if latest:
        return _json(ingest_latest(con))
    if not date_from or not date_to:
        return (
            "Error: provide latest=true, api_stations/api_prices=true, "
            "or both date_from and date_to"
        )
    return _json(ingest_date_range(con, date.fromisoformat(date_from), date.fromisoformat(date_to)))


@mcp.tool()
def database_stats() -> str:
    """Show database statistics: station count, price records, date range, ingestion history."""
    return _json(analysis.database_stats(_get_con()))


def main() -> None:
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
