import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.loader.s3_loader import S3Loader
from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team", default=None)
    parser.add_argument("--season", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--bronze-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--raw-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base_config = Config()
    config = base_config.for_team(args.team or base_config.TEAM_KEY)
    config = Config(
        TEAM_KEY=config.TEAM_KEY,
        CLUB_NAME=config.CLUB_NAME,
        TRANSFERMARKT_CLUB_SLUG=config.TRANSFERMARKT_CLUB_SLUG,
        TRANSFERMARKT_CLUB_ID=config.TRANSFERMARKT_CLUB_ID,
        TEAM_CONFIGS=config.TEAM_CONFIGS,
        S3_RAW_PREFIX=args.bronze_prefix or Config.S3_RAW_PREFIX,
        S3_CLEANED_PREFIX=args.silver_prefix or Config.S3_CLEANED_PREFIX,
    )

    try:
        loader = S3Loader(config=config)
        files = loader.collect_local_files(
            season=args.season,
            team=config.TEAM_KEY,
            include_cleaned=not args.raw_only,
        )
        uploaded_keys = loader.upload_files(
            files=files,
            bucket=args.bucket,
            dry_run=args.dry_run,
        )
        print(f"Files processed: {len(uploaded_keys)}")
    except Exception as exc:
        LOGGER.exception("Top-level S3 load run failed: %s", exc)


if __name__ == "__main__":
    main()
