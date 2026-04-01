# fuel-cartel-monitor

**Detect oligopolistic pricing patterns in German fuel markets using Tankerkoenig MTS-K data.**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Background

The German Federal Cartel Office (Bundeskartellamt) has documented that five major oil companies — **BP/Aral, Shell, ExxonMobil/Esso, TotalEnergies, and ConocoPhillips/Jet** — form a market-dominating oligopoly in the German retail fuel market. Their documented behavior:

- **Aral or Shell** initiates nationwide price increases (the "leaders")
- The other oligopolist follows within a characteristic **3-hour window**
- Remaining oligopolists follow in fixed time corridors
- Price **increases** are large and nationwide ("rockets")
- Price **decreases** are small and local ("feathers")

This tool ingests daily price data from the [Tankerkoenig open dataset](https://tankerkoenig.de/) and exposes analysis via an MCP (Model Context Protocol) server, making these patterns visible and queryable in real-time.

---

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/synapticore-io/fuel-cartel-monitor.git
cd fuel-cartel-monitor

# Install with uv
uv sync

# For development (includes pytest, ruff)
uv sync --extra dev
```

### Configuration

```bash
cp .env.example .env
# Edit .env with your Tankerkoenig credentials:
# TANKERKOENIG_DATA_USER=your-username
# TANKERKOENIG_DATA_PASS=your-api-key
```

Register for free at [creativecommons.tankerkoenig.de](https://creativecommons.tankerkoenig.de/) to obtain credentials.

---

## CLI Commands

### Ingest Data

```bash
# Ingest last 7 days of historical CSV data
fuel-cartel-monitor ingest --days 7

# Ingest a specific date range
fuel-cartel-monitor ingest --from 2026-03-01 --to 2026-03-31

# Fetch stations near a location via live API
fuel-cartel-monitor ingest --api-stations --lat 52.37 --lng 9.73 --radius 25

# Snapshot current prices for all stations in DB
fuel-cartel-monitor ingest --api-prices
```

### Analyze Pricing Patterns

All analysis commands default to **Hannover** (lat=52.37, lng=9.73) as the region center.

```bash
# Detect leader-follower patterns (which brand initiates, how fast others follow)
fuel-cartel-monitor analyze leader-follower --lat 52.37 --lng 9.73 --radius 25 --fuel e5 --days 30

# Detect rockets-and-feathers asymmetry (fast rises, slow falls)
fuel-cartel-monitor analyze rockets-feathers --lat 52.37 --lng 9.73 --fuel diesel --days 60

# Price synchronization index (coordinated pricing signal)
fuel-cartel-monitor analyze sync --lat 52.37 --lng 9.73 --days 30

# Brent crude decoupling (retail price vs oil cost)
fuel-cartel-monitor analyze brent-decoupling --fuel diesel --days 90

# Regional price comparison (by postal code)
fuel-cartel-monitor analyze regional --fuel e5
```

### Export Dashboard Data

```bash
# Export analysis results as JSON for GitHub Pages dashboard
fuel-cartel-monitor export --days 30 --fuel e5
```

### Database Stats

```bash
fuel-cartel-monitor stats
```

### Start MCP Server

```bash
fuel-cartel-monitor serve
```

---

## Analysis Types

### 1. Leader-Follower Lag

Identifies which brand initiates price changes and how quickly competing oligopolists follow. The Bundeskartellamt documented a characteristic **~3-hour lag** between Aral/Shell initiating an increase and the remaining oligopolists matching it.

**Output:** `leader_brand`, `follower_brand`, `median_lag_minutes`, `event_count`

### 2. Rockets and Feathers

Tests the asymmetric price transmission hypothesis: prices rise fast (like rockets) when crude oil prices increase, but fall slowly (like feathers) when crude prices decrease. An `asymmetry_ratio > 1.0` confirms the pattern.

**Output:** `brand`, `avg_increase_cents`, `avg_decrease_cents`, `avg_increase_speed_min`, `avg_decrease_speed_min`, `asymmetry_ratio`

### 3. Price Synchronization Index

Calculates pairwise price correlations between stations in a region. High synchronization between oligopolist pairs vs. low synchronization with independent stations is a statistical signal of coordinated pricing.

**Output:** `pair_brand_a`, `pair_brand_b`, `correlation`, `is_oligopol_pair`, `region_sync_index`

### 4. Brent Decoupling

Tracks the spread between retail fuel prices and Brent crude oil prices (in EUR/litre). A z-score > 2.0 flags days with abnormally wide spreads, potentially indicating margin extraction.

**Output:** `date`, `retail_avg`, `brent_eur`, `spread`, `spread_z_score`, `is_abnormal`

### 5. Regional Price Comparison

Compares average fuel prices by German postal code region (2-digit prefix) against the national average. Highlights which regions consistently pay a premium.

**Output:** `region_code`, `date`, `regional_avg`, `national_avg`, `premium_cents`

---

## MCP Server

The MCP server exposes all analysis functions as tools for use with Claude Desktop or any MCP-compatible client.

### Claude Desktop Configuration

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "fuel-cartel-monitor": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/fuel-cartel-monitor",
        "run",
        "fuel-cartel-monitor",
        "serve"
      ],
      "env": {
        "TANKERKOENIG_DATA_USER": "your-username",
        "TANKERKOENIG_DATA_PASS": "your-api-key"
      }
    }
  }
}
```

Tools with visual output (leader-follower, rockets-feathers, brent-decoupling) render interactive Chart.js charts directly in Claude Desktop via [MCP UI](https://mcpui.dev/).

### Available MCP Tools

| Tool | Description |
|------|-------------|
| `analyze_leader_follower` | Leader-follower lag analysis |
| `analyze_rockets_feathers` | Asymmetric price transmission |
| `analyze_price_sync` | Price synchronization index |
| `analyze_brent_decoupling` | Retail vs Brent crude spread |
| `compare_regions` | Regional price comparison |
| `station_price_history` | Price history for a station |
| `ingest_data` | Ingest Tankerkoenig CSV data |
| `database_stats` | Database statistics |

---

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Lint and format
uv run ruff check src tests
uv run ruff format src tests
```

---

## Data Sources

### Tankerkoenig MTS-K Data

Historical CSV data from the German Market Transparency Unit for Fuels (Markttransparenzstelle für Kraftstoffe, MTS-K), provided via the Tankerkoenig project.

- **Source:** [data.tankerkoenig.de](https://data.tankerkoenig.de/)
- **License:** [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (non-commercial)
- **Attribution:** Tankerkoenig · Bundeskartellamt — MTS-K

### Brent Crude Oil Prices

- **Source:** [EIA (U.S. Energy Information Administration)](https://www.eia.gov/)
- **EUR/USD Rates:** [ECB Statistical Data Warehouse](https://data.ecb.europa.eu/)

---

## License

MIT License — see [LICENSE](LICENSE) for details.

Copyright (c) 2026 synapticore.io
