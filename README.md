# fuel-price-monitor

![fuel-price-monitor hero](assets/fuel_price_monitor_hero.jpg)

**Wohin geht dein Tank-Euro? Monatliche Aufschlüsselung der deutschen Tankstellenpreise.**

[![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Dashboard](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-green.svg)](https://synapticore-io.github.io/fuel-price-monitor/)

**[→ Live-Dashboard](https://synapticore-io.github.io/fuel-price-monitor/)**

> *Symbolbild: KI-generiertes Tankstellen-Schild mit ~2023er Preisen — der ironische Kontrast zu den aktuellen 2,27 €/L Diesel im verlinkten Dashboard ist Teil der Pointe.*

Der Monatsdurchschnittspreis pro Liter Diesel und Super E5 wird in seine
gesetzlich fixen, marktbedingten und residualen Komponenten zerlegt:

- **Energiesteuer** (EnergieStG, fest pro Liter — Diesel 47,04 ct, E5 65,45 ct)
- **CO₂-Abgabe** (BEHG §10, 2026 Mindestpreis 55 €/t × UBA-Emissionsfaktor)
- **Rohöl Brent** (Monats-Spotmittel, EUR/Liter)
- **Mehrwertsteuer** (19 % auf den Bruttopreis)
- **Raffinerie + Händlermarge** (Residuum)

Datengrundlage: 13 Mio.+ Preisänderungen aus der Markttransparenzstelle für
Kraftstoffe (MTS-K) des Bundeskartellamts via Tankerkönig, Brent-Spotpreise
von CrudePriceAPI/EIA, EUR/USD-Kurse von der EZB.

## Stack

- **DuckDB** für lokale Analyse, **Parquet** für Monats-Archive
- **Python 3.12** mit `uv` Package-Manager, `httpx`, `duckdb`
- **GitHub Pages** für statisches Dashboard, Chart.js für Visualisierung

## Quickstart

```bash
uv sync
uv run python -m fuel_price_monitor.cli ingest --days 30
uv run python -m fuel_price_monitor.cli ingest --brent
uv run python -m fuel_price_monitor.cli export --month 2026-04
```

Voraussetzung: `.env` mit `TANKERKOENIG_DATA_USER`, `TANKERKOENIG_DATA_PASS`,
optional `CRUDE_PRICE_API_KEY` für aktuellen Brent.

## Subkommandos

| Befehl | Zweck |
|--------|-------|
| `ingest --days N` | Letzte N Tage Tankerkönig-CSVs (Skip via `ingestion_log`) |
| `ingest --brent` | Brent + EUR/USD nachladen |
| `export --month YYYY-MM` | Schreibt `docs/data/dashboard-YYYY-MM.json` + `index.json` |
| `archive --month YYYY-MM` | Monatsarchiv als zstd-Parquet |
| `analyze {leader-follower,rockets-feathers,sync,brent-decoupling,breakdown}` | Einzelne Analyse |
| `stats` | DB-Statistiken |

## Deployment

GitHub Action `update-dashboard.yml` läuft täglich 18:15 UTC: ingestiert die
letzten 60 Tage, holt Brent, exportiert alle Monate mit Daten als JSON
(`--all-months`), committed und pusht. GitHub Pages baut `docs/` automatisch.

## Lizenz

MIT für Code. Tankerkönig-Daten unter CC BY-NC-SA 4.0.

---

*Created with [Claude Code](https://www.anthropic.com/claude-code) by Anthropic.*
