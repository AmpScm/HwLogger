# hwLogger

An async Python daemon that logs energy data from HomeWizard P1 meters via WebSocket, stores it in SQLite with 5-minute and 30-second aggregates, and provides a REST API for querying.

## Features

- **Real-time WebSocket** connection to HomeWizard P1 APIv2 with proper authentication
- **Windowed aggregates**: 5-minute blocks with weighted averages and maximums (indefinite storage), plus 30-second snapshots (24h rolling)
- **Automatic gap filling**: Detects outages and backfills missing blocks with interpolated energy data for accurate reporting
- **Minimal REST API** for querying meter data by timestamp range
- **Async I/O** throughout using `asyncio` and `aiosqlite`
- **Command-line setup**: `--host` and `--token` arguments with persistent storage in database
- **Automatic user registration**: Button-click registration on P1 meter
- **Clean logging**: INFO for connection events/errors, DEBUG for normal data flow

## Quick Start

### Prerequisites

- Python 3.13+
- HomeWizard P1 meter on your network

### Installation

```bash
pip install -r requirements.txt
python run_daemon.py --host <P1_IP> --token <API_TOKEN>
```

Or register automatically:
```bash
python run_daemon.py --host <P1_IP>
# Press the registration button on the meter when prompted
```

## Usage

The daemon stores data in `meter_data.db`:

### meter_data table (5-minute aggregates)
- **Energy**: `timestamp`, `tariff`, `import_kwh`, `export_kwh`, `import_t1_kwh`, `import_t2_kwh`
- **Power**: `w` (total), `l1_w`, `l2_w`, `l3_w` (phase watts)
- **Voltage**: `l1_v`, `l2_v`, `l3_v`
- **Aggregates**: `*_avg` (1 decimal), `*_max` (2 decimals)
- **Metadata**: `reading_count` (0 = synthetic/backfilled block)

### meter_data_30s table (30-second snapshots)
Same schema as meter_data, rotates after 24 hours.

### REST API

Query meter data:
```bash
GET http://localhost:8080/api/v1/meter?start=2026-01-13T14:00:00&end=2026-01-13T15:00:00
```

## How It Works

1. **WebSocket stream** → Real-time measurements from P1 meter (~1/sec)
2. **30-second boundaries** → Calculate weighted averages, store snapshot
3. **5-minute boundaries** → Calculate weighted averages from accumulated readings, store permanently
4. **Hourly cleanup** → Remove 30-second data older than 24 hours
5. **Gap detection** → On startup, detect outages and backfill with synthetic blocks
6. **Energy interpolation** → When first real reading arrives after gap, interpolate actual consumption backwards into synthetic blocks

### Weighted Averaging

Each reading is weighted by its duration until the next reading. This smooths out measurement intervals and provides realistic average power consumption.

### Backfilling

If the daemon was offline for 1-30 minutes:
1. Creates synthetic 5-minute blocks with last-known energy values
2. On first real reading, calculates actual energy consumed during gap
3. Interpolates consumption proportionally across synthetic blocks
4. Marks synthetic blocks with `reading_count=0` for identification

## Configuration

P1 device host and API token are stored in SQLite `config` table after first setup:

```bash
# Setup with command line
python run_daemon.py --host 192.168.1.100 --token <token>

# Use stored config (no args needed)
python run_daemon.py
```

## Technology Stack

- **Python 3.13+** with asyncio
- **websockets 15.0.1** - WebSocket client with HomeWizard auth protocol
- **aiohttp 3.11+** - REST API server
- **aiosqlite 0.18+** - Async SQLite operations

## Development

```bash
# Validate syntax
python -m py_compile hwLogger/*.py

# Run with verbose logging
LOGLEVEL=DEBUG python run_daemon.py

# Query data directly
sqlite3 meter_data.db "SELECT timestamp, w_avg, w_max FROM meter_data LIMIT 10;"
```

## Database Schema

22 columns in both `meter_data` and `meter_data_30s`:

| Column | Type | NOT NULL | Notes |
|--------|------|----------|-------|
| timestamp | TEXT | ✓ | Window-end time (ISO8601 + microseconds) |
| tariff | INT | | Current tariff (1 or 2) |
| import_kwh | REAL | ✓ | Total import energy (kWh) |
| import_t1_kwh | REAL | ✓ | Tariff 1 import (kWh) |
| import_t2_kwh | REAL | ✓ | Tariff 2 import (kWh) |
| export_kwh | REAL | ✓ | Total export energy (kWh) |
| w | REAL | ✓ | Current power (W) |
| l1_w, l2_w, l3_w | REAL | | Phase power (W) |
| l1_v, l2_v, l3_v | REAL | | Phase voltage (V) |
| w_avg, l1_w_avg, l2_w_avg, l3_w_avg | REAL | | Weighted average power (1 decimal) |
| w_max, l1_w_max, l2_w_max, l3_w_max | REAL | | Max power (2 decimals) |
| reading_count | INT | ✓ | Samples in block (0 = synthetic) |

## License

See LICENSE file.
