import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.loader.local_bronze import save_local_individual_json
from src.scraper.scrape_roster import TeamRosterScraper
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
        scraper = TeamRosterScraper(config=config)
        payloads = []
        for season in seasons:
            players = scraper.get_squad_players(season)
            LOGGER.info(
                "Discovered %s %s players for season %s",
                len(players),
                config.CLUB_NAME,
                season,
            )
            payload = scraper.build_roster_payload(season, players)
            save_local_individual_json(
                data=payload,
                source="transfermarkt",
                team=config.TEAM_KEY,
                artifact_name="team_roster",
                season=season,
                config=config,
            )
            payloads.append(payload)
        total_players = sum(len(payload["players"]) for payload in payloads)
        print(f"Roster files written: {len(payloads)}")
        print(f"Total squad entries scraped: {total_players}")
    except Exception as exc:
        LOGGER.exception("Top-level team roster run failed: %s", exc)


if __name__ == "__main__":
    main()
