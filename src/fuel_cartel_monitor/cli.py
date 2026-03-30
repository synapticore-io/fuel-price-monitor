"""CLI entry point for fuel-cartel-monitor."""
import argparse
import json
import logging
import sys
from datetime import date, timedelta

from fuel_cartel_monitor import analysis
from fuel_cartel_monitor.db import get_connection
from fuel_cartel_monitor.ingest import ingest_date_range, ingest_latest

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Handle the 'ingest' subcommand."""
    con = get_connection()

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
    import asyncio

    from fuel_cartel_monitor.server import main

    asyncio.run(main())


def cmd_stats(_args: argparse.Namespace) -> None:
    """Handle the 'stats' subcommand."""
    con = get_connection()
    stats = analysis.database_stats(con)
    print(json.dumps(stats, indent=2, default=str))


def main() -> None:
    """Main entry point for the CLI."""
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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
