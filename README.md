# Fivetran-Style Resumable ELT Pipeline

A resilient, integration-style batch ELT pipeline built with Python, FastAPI, and PostgreSQL that mimics Fivetran/Airbyte-style cursor checkpointing, atomic file writes, and idempotent data loading.

## Architecture

```
                 +-------------------+
                 |     Mock API      |
                 |  GET /transactions|
                 +---------+---------+
                           |
                     (HTTP JSON)
                           v
                 +-------------------+
                 |     Extractor     |
                 +----+---------+----+
                      |         |
      (Atomic Writes) |         | (State Updates)
                      v         v
+-----------------------+     +-------------------+
| Landing File System   |     | State Checkpoint  |
| dt=YYYY-MM-DD/part_N  |     |   data/state.json |
+-----------+-----------+     +-------------------+
            |
            v
  +-------------------+
  | PostgreSQL Loader |
  | (UPSERT / ON CONF)|
  +---------+---------+
            v
  +-------------------+
  | PostgreSQL DB     |
  | table:transactions|
  +-------------------+
```

---

## Features & Implementation Highlights

1. **Mock Source API (`mock_api/`)**
   - Built with FastAPI (`GET /api/transactions?page=N`).
   - Serves exactly 10 pages of 100 unique transaction records (1000 total).
   - Serves empty data payload with `next_page: null` when `page > 10`.

2. **Partitioned Landing Zone & Atomic File Writes (`pipeline/extractor.py`)**
   - Writes API pages to `data/landing/transactions/dt=YYYY-MM-DD/part_{page}.json`.
   - Uses `write_atomically` with `.tmp` files and kernel-level `os.replace` to prevent partial/corrupt file writes during mid-execution crashes.

3. **State Management & Checkpointing (`pipeline/state.py`)**
   - Maintains execution state in `data/state.json`.
   - Tracks `last_page_extracted` and `updated_at`.
   - Atomic state file writes ensure checkpoint integrity.

4. **Crash Simulation & Resumability (`pipeline/run_elt.py`)**
   - Supports `--crash-after-page N` flag.
   - Saves `part_N.json`, updates `state.json`, and exits with return code `1`.
   - Upon restart, extraction reads `state.json` and resumes from page `N+1` without modifying or reloading previous partitions (`part_1.json` through `part_N.json` modification timestamps remain unchanged).

5. **Idempotent Warehouse Loader (`pipeline/loader.py`)**
   - Loads landed partitions into PostgreSQL `transactions` table.
   - Uses `INSERT ... ON CONFLICT (id) DO UPDATE` to guarantee exactly 1000 rows regardless of how many times the loader is executed.
   - Gracefully handles database connection failures by logging to `stderr` and exiting with a non-zero exit code.

6. **Environment Configuration (`.env`)**
   - All connection parameters (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `API_BASE_URL`) dynamically read from environment variables.

---

## Directory Structure

```
.
├── docker-compose.yml         # Container configuration for Postgres & Mock API
├── init.sql                   # Database initialization script
├── .env                       # Environment variables
├── .env.example               # Template environment configuration
├── requirements.txt           # Python dependencies
├── README.md                  # System documentation
├── mock_api/                  # Mock FastAPI service
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── pipeline/                  # Core ELT implementation
│   ├── run_elt.py             # CLI entrypoint script
│   ├── extractor.py           # API pagination & atomic partition writer
│   ├── loader.py              # Idempotent PostgreSQL loader
│   ├── state.py               # Checkpoint state manager
│   └── requirements.txt
└── tests/                     # Automated test suite
    └── test_pipeline.py
```

---

## How to Run

### 1. Start Docker Services
```bash
docker-compose up -d --build
```

### 2. Run Pipeline Normal Mode
```bash
python pipeline/run_elt.py
```

### 3. Crash & Resume Verification Scenario
```bash
# Step A: Simulate mid-run crash after page 2
python pipeline/run_elt.py --crash-after-page 2

# Step B: Resume pipeline execution
python pipeline/run_elt.py
```

### 4. Run Automated Test Suite
```bash
pytest -v
```
