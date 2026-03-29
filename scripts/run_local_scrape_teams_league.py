import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.scraper.scrape_league import LeagueScraper
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--league-id", default="GB1")
    parser.add_argument("--seasons", nargs="+", default=["2025"])
    args = parser.parse_args()

    try:
        scraper = LeagueScraper()
        teams_by_id: dict[str, dict[str, str]] = {}
        for season in args.seasons:
            teams = scraper.scrape_teams(args.league_id, season)
            LOGGER.info(
                "Discovered %s teams for league %s in season %s",
                len(teams),
                args.league_id,
                season,
            )
            for team in teams:
                teams_by_id.setdefault(team["club_id"], team)

        print(f"League: {args.league_id}")
        print(f"Seasons: {', '.join(args.seasons)}")
        print(f"Teams found: {len(teams_by_id)}")
        for team in sorted(teams_by_id.values(), key=lambda item: item["club_name"]):
            print(f"- {team['club_name']} ({team['club_id']})")
    except Exception as exc:
        LOGGER.exception("Top-level league scrape run failed: %s", exc)


if __name__ == "__main__":
    main()
