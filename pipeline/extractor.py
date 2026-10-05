import os
import sys
import json
import requests
from datetime import datetime, timezone
from pipeline.state import StateManager


def write_atomically(data, final_path: str):
    """
    Writes data to a temporary file, flushes/fsyncs it to disk,
    and atomically replaces final_path with os.replace.
    """
    dir_name = os.path.dirname(final_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    tmp_path = final_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, final_path)


class Extractor:
    def __init__(self, api_base_url: str, state_manager: StateManager, landing_base_dir: str = "data/landing"):
        self.api_base_url = api_base_url.rstrip("/")
        self.state_manager = state_manager
        self.landing_base_dir = landing_base_dir

    def extract_transactions(self, stream_name: str = "transactions", crash_after_page: int = None):
        last_page = self.state_manager.get_last_processed_page(stream_name)
        start_page = last_page + 1

        dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        partition_dir = os.path.join(self.landing_base_dir, stream_name, f"dt={dt_str}")

        current_page = start_page

        while current_page is not None:
            url = f"{self.api_base_url}/api/transactions?page={current_page}"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Error fetching page {current_page} from API: {e}", file=sys.stderr)
                raise e

            payload = response.json()
            data_items = payload.get("data", [])
            next_page = payload.get("next_page")

            if not data_items:
                break

            file_path = os.path.join(partition_dir, f"part_{current_page}.json")
            
            # Atomic file write as required by req-8
            write_atomically(data_items, file_path)

            # Update checkpoint state as required by req-3
            self.state_manager.update_state(stream_name, current_page)

            print(f"Successfully extracted and landed page {current_page} to {file_path}")

            if crash_after_page is not None and current_page == crash_after_page:
                print(f"Simulating crash after page {current_page} as requested.", file=sys.stderr)
                sys.exit(1)

            current_page = next_page
