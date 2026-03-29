import os
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lambda_deployment.scrape_players_handler import handler


def run_test() -> None:
    event = {
        "team": "manchester_united",
        "season": "2025",
        "scrape_date": datetime.now(timezone.utc).date().isoformat(),
        "player": "bruno fernandes",
        "competition": "GB1",
    }

    print("=== Running player lambda locally ===")

    result = handler(event, None)

    print("\n=== RESULT ===")
    print(result)


if __name__ == "__main__":
    os.environ.setdefault("S3_BUCKET", "sport-analysis")
    os.environ.setdefault("S3_RAW_PREFIX", "raw")
    run_test()
