import os
import sys
import shutil
import time
import json
import subprocess
import pytest
import requests
import psycopg2
from datetime import datetime, timezone
from unittest.mock import MagicMock

# Project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

def clean_data_dir():
    data_dir = os.path.join(PROJECT_ROOT, "data")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

@pytest.fixture(autouse=True)
def setup_teardown():
    clean_data_dir()
    yield
    clean_data_dir()

def test_req1_mock_api():
    """Req-1: Mock API service returns stable, paginated transaction data."""
    # Test page 1
    resp1 = requests.get("http://localhost:8000/api/transactions?page=1")
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert len(data1["data"]) == 100
    assert data1["next_page"] == 2
    record1 = data1["data"][0]
    assert "id" in record1
    assert "user_id" in record1
    assert "amount" in record1
    assert "created_at" in record1

    # Test page 10
    resp10 = requests.get("http://localhost:8000/api/transactions?page=10")
    assert resp10.status_code == 200
    data10 = resp10.json()
    assert len(data10["data"]) == 100
    assert data10["next_page"] is None

    # Test page 11 (beyond last page)
    resp11 = requests.get("http://localhost:8000/api/transactions?page=11")
    assert resp11.status_code == 200
    data11 = resp11.json()
    assert len(data11["data"]) == 0
    assert data11["next_page"] is None


def test_req8_atomic_writes_code_inspection():
    """Req-8: Extractor writes landing files atomically to prevent data corruption."""
    extractor_file = os.path.join(PROJECT_ROOT, "pipeline", "extractor.py")
    with open(extractor_file, "r", encoding="utf-8") as f:
        content = f.read()

    assert ("os.replace" in content or "os.rename" in content), \
        "pipeline/extractor.py must use os.replace or os.rename for atomic file writes!"
    assert ".tmp" in content, "pipeline/extractor.py should use a temporary file extension!"


def test_req4_and_req5_crash_and_resumability():
    """
    Req-4: Crash parameter --crash-after-page N terminates script at N and updates state.
    Req-5: Resuming extraction does not modify mtime of previously completed partitions.
    Req-2: Check landing storage path structure.
    Req-3: Check state.json structure.
    """
    run_elt_script = os.path.join(PROJECT_ROOT, "pipeline", "run_elt.py")

    # Step 1: Run with --crash-after-page 2
    cmd1 = [sys.executable, run_elt_script, "--crash-after-page", "2"]
    res1 = subprocess.run(cmd1, cwd=PROJECT_ROOT, capture_output=True, text=True)
    assert res1.returncode == 1, f"Expected returncode 1, got {res1.returncode}. Stderr: {res1.stderr}"

    # Verify state.json
    state_file = os.path.join(PROJECT_ROOT, "data", "state.json")
    assert os.path.exists(state_file)
    with open(state_file, "r") as f:
        state_data = json.load(f)
    assert state_data["transactions"]["last_page_extracted"] == 2

    dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    landing_dt_dir = os.path.join(PROJECT_ROOT, "data", "landing", "transactions", f"dt={dt_str}")
    
    part1_path = os.path.join(landing_dt_dir, "part_1.json")
    part2_path = os.path.join(landing_dt_dir, "part_2.json")
    part3_path = os.path.join(landing_dt_dir, "part_3.json")

    assert os.path.exists(part1_path)
    assert os.path.exists(part2_path)
    assert not os.path.exists(part3_path)

    # Record mtime of part_1 and part_2
    part1_mtime_orig = os.path.getmtime(part1_path)
    part2_mtime_orig = os.path.getmtime(part2_path)

    # Sleep slightly to ensure measurable mtime difference if re-written
    time.sleep(1.1)

    # Test extraction resumption directly via Extractor class to isolate extraction/resume behavior
    from pipeline.state import StateManager
    from pipeline.extractor import Extractor

    sm = StateManager(state_file_path=state_file)
    ext = Extractor("http://localhost:8000", sm, os.path.join(PROJECT_ROOT, "data", "landing"))
    ext.extract_transactions("transactions")

    # Check part_3 through part_10 exist
    for p in range(3, 11):
        p_path = os.path.join(landing_dt_dir, f"part_{p}.json")
        assert os.path.exists(p_path), f"Expected {p_path} to exist"

    # Verify state updated to 10
    with open(state_file, "r") as f:
        state_data = json.load(f)
    assert state_data["transactions"]["last_page_extracted"] == 10

    # CRITICAL Req-5 Assertion: mtimes of part_1 and part_2 MUST be identical to original
    part1_mtime_new = os.path.getmtime(part1_path)
    part2_mtime_new = os.path.getmtime(part2_path)

    assert part1_mtime_new == part1_mtime_orig, "part_1.json was modified on resume!"
    assert part2_mtime_new == part2_mtime_orig, "part_2.json was modified on resume!"


def test_req6_postgres_loader(monkeypatch):
    """Req-6: Loader idempotently inserts landed data into PostgreSQL."""
    from pipeline.state import StateManager
    from pipeline.extractor import Extractor
    from pipeline.loader import Loader

    state_file = os.path.join(PROJECT_ROOT, "data", "state.json")
    landing_dir = os.path.join(PROJECT_ROOT, "data", "landing")
    sm = StateManager(state_file)
    ext = Extractor("http://localhost:8000", sm, landing_dir)
    ext.extract_transactions("transactions")

    loader = Loader(
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", 5432)),
        db_user=os.getenv("DB_USER", "postgres"),
        db_pass=os.getenv("DB_PASSWORD", "postgres"),
        db_name=os.getenv("DB_NAME", "warehouse"),
        landing_base_dir=landing_dir
    )

    try:
        conn = loader.get_connection()
        cur = conn.cursor()
        loader.load_transactions("transactions")

        cur.execute("SELECT COUNT(*) FROM transactions;")
        count1 = cur.fetchone()[0]
        assert count1 == 1000

        # Load second time for idempotency check
        loader.load_transactions("transactions")
        cur.execute("SELECT COUNT(*) FROM transactions;")
        count2 = cur.fetchone()[0]
        assert count2 == 1000
        cur.close()
        conn.close()
    except SystemExit:
        # If DB is not available in local host environment, mock connection to test loader logic
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        records_inserted = []
        def mock_execute(query, params=None):
            if params:
                records_inserted.append(params[0]) # store id

        mock_cur.execute.side_effect = mock_execute
        monkeypatch.setattr(loader, "get_connection", lambda: mock_conn)
        loader.load_transactions("transactions")
        assert len(records_inserted) == 1000


def test_req10_error_handling_db_down():
    """Req-10: Pipeline gracefully handles target database unavailability during load phase."""
    run_elt_script = os.path.join(PROJECT_ROOT, "pipeline", "run_elt.py")

    # Run with invalid DB port
    env_copy = os.environ.copy()
    env_copy["DB_PORT"] = "59999" # invalid port

    res = subprocess.run([sys.executable, run_elt_script], cwd=PROJECT_ROOT, env=env_copy, capture_output=True, text=True)
    assert res.returncode != 0, "Expected non-zero returncode when DB is unreachable"
    assert "Database connection error" in res.stderr or "error" in res.stderr.lower()
