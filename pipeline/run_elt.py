import os
import sys
import argparse
from dotenv import load_dotenv

# Add parent directory to sys.path to ensure module imports work when invoked from anywhere
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from pipeline.state import StateManager
from pipeline.extractor import Extractor
from pipeline.loader import Loader


def main():
    parser = argparse.ArgumentParser(description="Resumable ELT Pipeline")
    parser.add_argument(
        "--crash-after-page",
        type=int,
        default=None,
        help="Simulate a crash immediately after processing and landing page N"
    )
    args = parser.parse_args()

    # Load environment variables from .env file
    load_dotenv()

    # Environment variables (Requirement 9)
    api_base_url = os.environ.get("API_BASE_URL", "http://localhost:8000")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = int(os.environ.get("DB_PORT", "5432"))
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    db_name = os.environ.get("DB_NAME", "warehouse")

    state_file = os.path.join("data", "state.json")
    landing_dir = os.path.join("data", "landing")

    state_manager = StateManager(state_file_path=state_file)
    extractor = Extractor(api_base_url=api_base_url, state_manager=state_manager, landing_base_dir=landing_dir)
    loader = Loader(
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_pass=db_password,
        db_name=db_name,
        landing_base_dir=landing_dir
    )

    print(f"Starting Extraction phase from {api_base_url}...")
    extractor.extract_transactions("transactions", crash_after_page=args.crash_after_page)

    print("Starting Load phase into PostgreSQL...")
    loader.load_transactions("transactions")

    print("ELT Pipeline completed successfully!")


if __name__ == "__main__":
    main()
