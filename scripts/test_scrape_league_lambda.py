from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lambda_deployment.scrape_teams_league_handler import handler


def run_test() -> None:
    event = {
        "league_id": "GB1",
        "seasons": ["2025"],
    }

    print("=== Running league scrape lambda locally ===")
    result = handler(event, None)
    print("\n=== RESULT ===")
    print(result)


if __name__ == "__main__":
    run_test()
