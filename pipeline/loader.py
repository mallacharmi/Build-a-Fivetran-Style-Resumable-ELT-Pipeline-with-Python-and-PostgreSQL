import os
import sys
import json
import glob
import psycopg2

class Loader:
    def __init__(self, db_host: str, db_port: int, db_user: str, db_pass: str, db_name: str, landing_base_dir: str = "data/landing"):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.landing_base_dir = landing_base_dir

    def get_connection(self):
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                dbname=self.db_name,
                connect_timeout=5
            )
            return conn
        except psycopg2.Error as e:
            # Req-10: Must log descriptive error to stderr and exit with non-zero exit code
            sys.stderr.write(f"Database connection error: Unable to connect to PostgreSQL at {self.db_host}:{self.db_port}/{self.db_name}. Details: {e}\n")
            sys.stderr.flush()
            sys.exit(1)
        except Exception as e:
            sys.stderr.write(f"Unexpected database connection error: {e}\n")
            sys.stderr.flush()
            sys.exit(1)

    def load_transactions(self, stream_name: str = "transactions"):
        conn = self.get_connection()
        cursor = conn.cursor()

        # Find all JSON files in the landing directory recursively
        pattern = os.path.join(self.landing_base_dir, stream_name, "**", "part_*.json")
        json_files = sorted(glob.glob(pattern, recursive=True))

        if not json_files:
            print(f"No landed files found to load for stream '{stream_name}'.")
            cursor.close()
            conn.close()
            return

        upsert_query = """
            INSERT INTO transactions (id, user_id, amount, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                amount = EXCLUDED.amount,
                created_at = EXCLUDED.created_at;
        """

        total_loaded = 0
        for file_path in json_files:
            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)

            for item in records:
                cursor.execute(
                    upsert_query,
                    (item["id"], item["user_id"], item["amount"], item["created_at"])
                )
                total_loaded += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully loaded {total_loaded} records (upserted) into database.")
