import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.cleaner.transform_player_stats import PlayerStatsTransformer
from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team", default=None)
    parser.add_argument("--season", default=None)
    args = parser.parse_args()

    base_config = Config()
    config = base_config.for_team(args.team or base_config.TEAM_KEY)
    seasons = [args.season] if args.season else config.SEASONS

    try:
        transformer = PlayerStatsTransformer(config=config)
        written_paths = transformer.transform_seasons(seasons=seasons)
        print(f"Cleaned files written: {len(written_paths)}")
    except Exception as exc:
        LOGGER.exception("Top-level clean player stats run failed: %s", exc)


if __name__ == "__main__":
    main()
