"""CLI entry point for fuel-cartel-monitor."""
import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from fuel_cartel_monitor import analysis
from fuel_cartel_monitor.brent import ingest_brent
from fuel_cartel_monitor.db import get_connection
from fuel_cartel_monitor.ingest import (
    ingest_date_range,
    ingest_latest,
    ingest_prices_api,
    ingest_stations_api,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Handle the 'ingest' subcommand."""
    con = get_connection()

    if args.api_stations:
        count = ingest_stations_api(con, lat=args.lat, lng=args.lng, radius_km=args.radius)
        print(json.dumps({"stations_ingested": count}))
        return

    if args.api_prices:
        count = ingest_prices_api(con)
        print(json.dumps({"prices_ingested": count}))
        return

    if args.brent:
        d_from = (
            date.fromisoformat(args.date_from)
            if args.date_from
            else date.today() - timedelta(days=90)
        )
        d_to = date.fromisoformat(args.date_to) if args.date_to else date.today()
        count = ingest_brent(con, d_from, d_to)
        print(json.dumps({"brent_records": count}))
        return

    if args.latest:
        result = ingest_latest(con)
        print(json.dumps(result, indent=2, default=str))
        return

    today = date.today()

    if args.days:
        date_from = today - timedelta(days=args.days - 1)
        date_to = today - timedelta(days=1)
    elif args.date_from and args.date_to:
        date_from = date.fromisoformat(args.date_from)
        date_to = date.fromisoformat(args.date_to)
    elif args.date_from:
        date_from = date.fromisoformat(args.date_from)
        date_to = today - timedelta(days=1)
    else:
        print(
            "Error: specify --latest, --days N, or --from DATE [--to DATE]",
            file=sys.stderr,
        )
        sys.exit(1)

    result = ingest_date_range(con, date_from, date_to)
    print(json.dumps(result, indent=2, default=str))


def cmd_analyze(args: argparse.Namespace) -> None:
    """Handle the 'analyze' subcommand."""
    con = get_connection()

    if args.type == "leader-follower":
        results = analysis.leader_follower_lag(
            con,
            lat=args.lat,
            lng=args.lng,
            radius_km=args.radius,
            fuel_type=args.fuel,
            lookback_days=args.days,
        )
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "rockets-feathers":
        results = analysis.rockets_and_feathers(
            con,
            lat=args.lat,
            lng=args.lng,
            radius_km=args.radius,
            fuel_type=args.fuel,
            lookback_days=args.days,
        )
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "sync":
        result = analysis.price_sync_index(
            con,
            lat=args.lat,
            lng=args.lng,
            radius_km=args.radius,
            fuel_type=args.fuel,
            lookback_days=args.days,
        )
        print(json.dumps(result, indent=2))

    elif args.type == "brent-decoupling":
        results = analysis.brent_decoupling(
            con,
            fuel_type=args.fuel,
            lookback_days=args.days,
        )
        print(json.dumps([vars(r) for r in results], indent=2))

    elif args.type == "regional":
        results = analysis.regional_comparison(
            con,
            fuel_type=args.fuel,
        )
        print(json.dumps(results, indent=2))

    else:
        print(f"Error: unknown analysis type: {args.type!r}", file=sys.stderr)
        sys.exit(1)


def cmd_serve(_args: argparse.Namespace) -> None:
    """Handle the 'serve' subcommand — start the MCP server."""
    from fuel_cartel_monitor.server import main

    main()


def cmd_stats(_args: argparse.Namespace) -> None:
    """Handle the 'stats' subcommand."""
    con = get_connection()
    stats = analysis.database_stats(con)
    print(json.dumps(stats, indent=2, default=str))


def cmd_export(args: argparse.Namespace) -> None:
    """Handle the 'export' subcommand — export analysis results as JSON for dashboard."""
    con = get_connection()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    regions = [
        {"name": "Hannover", "lat": 52.37, "lng": 9.73},
        {"name": "Hamburg", "lat": 53.55, "lng": 9.99},
        {"name": "Berlin", "lat": 52.52, "lng": 13.41},
        {"name": "München", "lat": 48.14, "lng": 11.58},
        {"name": "Köln", "lat": 50.94, "lng": 6.96},
    ]

    radius = args.radius
    days = args.days
    fuel = args.fuel

    dashboard = {
        "generated_at": datetime.now().isoformat(),
        "parameters": {"radius_km": radius, "lookback_days": days, "fuel_type": fuel},
        "stats": analysis.database_stats(con),
        "regions": {},
    }

    for region in regions:
        name = region["name"]
        lat, lng = region["lat"], region["lng"]
        logger.info("Exporting %s (lat=%s, lng=%s)", name, lat, lng)

        lf = analysis.leader_follower_lag(con, lat, lng, radius, fuel, days)
        rf = analysis.rockets_and_feathers(con, lat, lng, radius, fuel, days)

        dashboard["regions"][name] = {
            "lat": lat,
            "lng": lng,
            "leader_follower": [vars(r) for r in lf],
            "rockets_feathers": [vars(r) for r in rf],
        }

    dashboard["best_time"] = analysis.best_time_to_tank(con, fuel)
    dashboard["brand_ranking"] = analysis.brand_ranking(con, fuel)
    dashboard["consumer_impact"] = analysis.consumer_impact(con, fuel)

    for ft in ["diesel", "e5"]:
        decoupling = analysis.brent_decoupling(con, ft, days)
        dashboard[f"brent_decoupling_{ft}"] = [vars(r) for r in decoupling]

    if dashboard["brent_decoupling_diesel"]:
        d = dashboard["brent_decoupling_diesel"]
        first, last = d[0], d[-1]
        brent_rise = last["brent_eur"] - first["brent_eur"]
        retail_rise = last["retail_avg"] - first["retail_avg"]
        greed = retail_rise - brent_rise
        dashboard["greed_margin"] = {
            "diesel": {
                "brent_rise_cents": round(brent_rise * 100, 1),
                "retail_rise_cents": round(retail_rise * 100, 1),
                "greed_cents": round(greed * 100, 1),
                "greed_pct": round(greed / retail_rise * 100, 0) if retail_rise > 0 else 0,
                "cost_per_tank_50l": round(greed * 50, 2),
            },
        }
    if dashboard["brent_decoupling_e5"]:
        d = dashboard["brent_decoupling_e5"]
        first, last = d[0], d[-1]
        brent_rise = last["brent_eur"] - first["brent_eur"]
        retail_rise = last["retail_avg"] - first["retail_avg"]
        greed = retail_rise - brent_rise
        dashboard.setdefault("greed_margin", {})["e5"] = {
            "brent_rise_cents": round(brent_rise * 100, 1),
            "retail_rise_cents": round(retail_rise * 100, 1),
            "greed_cents": round(greed * 100, 1),
            "greed_pct": round(greed / retail_rise * 100, 0) if retail_rise > 0 else 0,
            "cost_per_tank_50l": round(greed * 50, 2),
        }

    data_path = out / "dashboard.json"
    data_path.write_text(json.dumps(dashboard, indent=2, default=str), encoding="utf-8")
    logger.info("Dashboard data exported to %s", data_path)
    print(json.dumps({"exported_to": str(data_path), "regions": len(regions)}))


def main() -> None:
    """Main entry point for the CLI."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="fuel-cartel-monitor",
        description="Detect oligopolistic pricing patterns in German fuel markets",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- ingest ---
    ingest_parser = subparsers.add_parser("ingest", help="Ingest Tankerkoenig CSV data")
    ingest_parser.add_argument(
        "--latest", action="store_true", help="Ingest the latest available day"
    )
    ingest_parser.add_argument("--from", dest="date_from", metavar="DATE", help="Start date (ISO)")
    ingest_parser.add_argument("--to", dest="date_to", metavar="DATE", help="End date (ISO)")
    ingest_parser.add_argument(
        "--days", type=int, metavar="N", help="Ingest the last N days"
    )
    ingest_parser.add_argument(
        "--api-stations", action="store_true",
        help="Fetch stations near --lat/--lng via Tankerkoenig live API",
    )
    ingest_parser.add_argument(
        "--api-prices", action="store_true",
        help="Snapshot current prices for all stations in DB via live API",
    )
    ingest_parser.add_argument(
        "--brent", action="store_true",
        help="Fetch Brent crude oil prices (use with --from/--to)",
    )
    ingest_parser.add_argument(
        "--lat", type=float, default=52.37, help="Latitude (default: Hannover)"
    )
    ingest_parser.add_argument(
        "--lng", type=float, default=9.73, help="Longitude (default: Hannover)"
    )
    ingest_parser.add_argument(
        "--radius", type=float, default=25.0, help="Radius in km (for --api-stations)"
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    # --- analyze ---
    analyze_parser = subparsers.add_parser("analyze", help="Run an analysis")
    analyze_parser.add_argument(
        "type",
        choices=["leader-follower", "rockets-feathers", "sync", "brent-decoupling", "regional"],
        help="Analysis type",
    )
    analyze_parser.add_argument(
        "--lat", type=float, default=52.37, help="Latitude (default: Hannover)"
    )
    analyze_parser.add_argument(
        "--lng", type=float, default=9.73, help="Longitude (default: Hannover)"
    )
    analyze_parser.add_argument("--radius", type=float, default=25.0, help="Radius in km")
    analyze_parser.add_argument(
        "--fuel", choices=["diesel", "e5", "e10"], default="e5", help="Fuel type"
    )
    analyze_parser.add_argument("--days", type=int, default=30, help="Lookback days")
    analyze_parser.set_defaults(func=cmd_analyze)

    # --- serve ---
    serve_parser = subparsers.add_parser("serve", help="Start the MCP server")
    serve_parser.set_defaults(func=cmd_serve)

    # --- stats ---
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # --- export ---
    export_parser = subparsers.add_parser("export", help="Export analysis as JSON for dashboard")
    export_parser.add_argument(
        "--output", default="docs/data", help="Output directory (default: docs/data)"
    )
    export_parser.add_argument("--radius", type=float, default=25.0, help="Radius in km")
    export_parser.add_argument(
        "--fuel", choices=["diesel", "e5", "e10"], default="e5", help="Fuel type"
    )
    export_parser.add_argument("--days", type=int, default=30, help="Lookback days")
    export_parser.set_defaults(func=cmd_export)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
