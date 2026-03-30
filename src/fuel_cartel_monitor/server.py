"""MCP server exposing fuel price analysis tools."""
import asyncio
import json
import logging

import duckdb
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from fuel_cartel_monitor import analysis
from fuel_cartel_monitor.db import get_connection
from fuel_cartel_monitor.ingest import ingest_date_range, ingest_latest

logger = logging.getLogger(__name__)

app = Server("fuel-cartel-monitor")

_con: duckdb.DuckDBPyConnection | None = None


def _get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = get_connection()
    return _con


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="analyze_leader_follower",
            description=(
                "Detect leader-follower pricing patterns between major oil brands in a region. "
                "Shows which brand initiates price changes and how quickly others follow. "
                "The Bundeskartellamt documented a typical 3-hour lag between Aral/Shell "
                "and followers."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "description": "Latitude of region center"},
                    "lng": {"type": "number", "description": "Longitude of region center"},
                    "radius_km": {
                        "type": "number",
                        "default": 25,
                        "description": "Search radius in km",
                    },
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "e5",
                    },
                    "lookback_days": {"type": "integer", "default": 30},
                },
                "required": ["lat", "lng"],
            },
        ),
        Tool(
            name="analyze_rockets_feathers",
            description=(
                "Detect asymmetric price transmission: prices rise fast (rockets) "
                "but fall slowly (feathers). An asymmetry ratio > 1.0 indicates the pattern."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "description": "Latitude of region center"},
                    "lng": {"type": "number", "description": "Longitude of region center"},
                    "radius_km": {"type": "number", "default": 25},
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "e5",
                    },
                    "lookback_days": {"type": "integer", "default": 30},
                },
                "required": ["lat", "lng"],
            },
        ),
        Tool(
            name="analyze_price_sync",
            description=(
                "Calculate price synchronization index for fuel stations in a region. "
                "High sync between oligopol members vs low sync with independents "
                "suggests coordination."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "lat": {"type": "number"},
                    "lng": {"type": "number"},
                    "radius_km": {"type": "number", "default": 25},
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "e5",
                    },
                    "lookback_days": {"type": "integer", "default": 30},
                },
                "required": ["lat", "lng"],
            },
        ),
        Tool(
            name="analyze_brent_decoupling",
            description=(
                "Track the gap between retail fuel prices and Brent crude oil price. "
                "Flags abnormal widening that may indicate margin extraction."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "diesel",
                    },
                    "lookback_days": {"type": "integer", "default": 90},
                },
            },
        ),
        Tool(
            name="compare_regions",
            description=(
                "Compare fuel prices across German regions (by postal code prefix). "
                "Shows regional premiums relative to national average."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "e5",
                    },
                    "date_from": {"type": "string", "description": "ISO date, e.g. 2025-01-01"},
                    "date_to": {"type": "string", "description": "ISO date, e.g. 2025-03-31"},
                },
            },
        ),
        Tool(
            name="station_price_history",
            description=(
                "Get price history for a specific fuel station by UUID or "
                "by searching near coordinates."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "station_uuid": {"type": "string", "description": "Station UUID"},
                    "lat": {"type": "number", "description": "Search near this latitude"},
                    "lng": {"type": "number", "description": "Search near this longitude"},
                    "fuel_type": {
                        "type": "string",
                        "enum": ["diesel", "e5", "e10"],
                        "default": "e5",
                    },
                    "days": {"type": "integer", "default": 7},
                },
            },
        ),
        Tool(
            name="ingest_data",
            description=(
                "Ingest Tankerkoenig CSV data into the database. "
                "Specify a date range or 'latest' to fetch the most recent day."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "date_from": {"type": "string", "description": "ISO date to start ingestion"},
                    "date_to": {"type": "string", "description": "ISO date to end ingestion"},
                    "latest": {
                        "type": "boolean",
                        "default": False,
                        "description": "Fetch only latest available day",
                    },
                },
            },
        ),
        Tool(
            name="database_stats",
            description=(
                "Show database statistics: number of stations, price records, "
                "date range, ingestion history."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Route tool calls to analysis functions."""
    con = _get_con()

    try:
        result = _dispatch_tool(name, arguments, con)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except ValueError as exc:
        return [TextContent(type="text", text=f"Error: {exc}")]
    except Exception as exc:
        logger.exception("Tool %s failed", name)
        return [TextContent(type="text", text=f"Error executing {name}: {exc}")]


def _dispatch_tool(name: str, arguments: dict, con: duckdb.DuckDBPyConnection) -> object:
    """Dispatch a tool call to the appropriate analysis function."""
    if name == "analyze_leader_follower":
        results = analysis.leader_follower_lag(
            con,
            lat=arguments["lat"],
            lng=arguments["lng"],
            radius_km=arguments.get("radius_km", 25.0),
            fuel_type=arguments.get("fuel_type", "e5"),
            lookback_days=arguments.get("lookback_days", 30),
        )
        return [vars(r) for r in results]

    elif name == "analyze_rockets_feathers":
        results = analysis.rockets_and_feathers(
            con,
            lat=arguments["lat"],
            lng=arguments["lng"],
            radius_km=arguments.get("radius_km", 25.0),
            fuel_type=arguments.get("fuel_type", "e5"),
            lookback_days=arguments.get("lookback_days", 30),
        )
        return [vars(r) for r in results]

    elif name == "analyze_price_sync":
        return analysis.price_sync_index(
            con,
            lat=arguments["lat"],
            lng=arguments["lng"],
            radius_km=arguments.get("radius_km", 25.0),
            fuel_type=arguments.get("fuel_type", "e5"),
            lookback_days=arguments.get("lookback_days", 30),
        )

    elif name == "analyze_brent_decoupling":
        results = analysis.brent_decoupling(
            con,
            fuel_type=arguments.get("fuel_type", "diesel"),
            lookback_days=arguments.get("lookback_days", 90),
        )
        return [vars(r) for r in results]

    elif name == "compare_regions":
        return analysis.regional_comparison(
            con,
            fuel_type=arguments.get("fuel_type", "e5"),
            date_from=arguments.get("date_from"),
            date_to=arguments.get("date_to"),
        )

    elif name == "station_price_history":
        return analysis.station_price_history(
            con,
            station_uuid=arguments.get("station_uuid"),
            lat=arguments.get("lat"),
            lng=arguments.get("lng"),
            fuel_type=arguments.get("fuel_type", "e5"),
            days=arguments.get("days", 7),
        )

    elif name == "ingest_data":
        if arguments.get("latest"):
            return ingest_latest(con)
        date_from_str = arguments.get("date_from")
        date_to_str = arguments.get("date_to")
        if not date_from_str or not date_to_str:
            raise ValueError("Either 'latest' or both 'date_from' and 'date_to' must be provided")
        from datetime import date as date_type

        date_from = date_type.fromisoformat(date_from_str)
        date_to = date_type.fromisoformat(date_to_str)
        return ingest_date_range(con, date_from, date_to)

    elif name == "database_stats":
        return analysis.database_stats(con)

    else:
        raise ValueError(f"Unknown tool: {name!r}")


async def main() -> None:
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
