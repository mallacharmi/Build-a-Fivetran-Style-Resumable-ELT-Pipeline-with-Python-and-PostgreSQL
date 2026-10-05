import os
import json
from datetime import datetime, timezone

class StateManager:
    def __init__(self, state_file_path: str = "data/state.json"):
        self.state_file_path = state_file_path

    def _ensure_dir(self):
        dir_name = os.path.dirname(self.state_file_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

    def _read_state(self) -> dict:
        if not os.path.exists(self.state_file_path):
            return {}
        try:
            with open(self.state_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def get_last_processed_page(self, stream_name: str = "transactions") -> int:
        state = self._read_state()
        stream_state = state.get(stream_name, {})
        return stream_state.get("last_page_extracted", 0)

    def update_state(self, stream_name: str, page: int):
        self._ensure_dir()
        state = self._read_state()
        state[stream_name] = {
            "last_page_extracted": page,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Atomic write for state file
        tmp_path = f"{self.state_file_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.state_file_path)
