import argparse
from pathlib import Path
import re
import sys
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.loader.local_bronze import save_local_combined_csv, save_local_individual_json
from src.scraper.scrape_player import PlayerLogScraper
from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def _normalize_player_selector(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _filter_players(players: list[dict[str, str]], player_selector: Optional[str]) -> list[dict[str, str]]:
    if not player_selector:
        return players

    normalized_selector = _normalize_player_selector(player_selector)
    return [
        player
        for player in players
        if _normalize_player_selector(player["player_name"]) == normalized_selector
        or _normalize_player_selector(player["player_slug"]) == normalized_selector
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team", default=None)
    parser.add_argument("--season", default=None)
    parser.add_argument("--player", default=None)
    parser.add_argument("--competition", default=None)
    args = parser.parse_args()

    base_config = Config()
    config = base_config.for_team(args.team or base_config.TEAM_KEY)
    seasons = [args.season] if args.season else config.SEASONS

    try:
        scraper = PlayerLogScraper(config=config)
        roster_count = 0
        player_count = 0
        total_rows = 0

        for season in seasons:
            season_rows = []
            squad_players = scraper.roster_scraper.get_squad_players(season)
            squad_players = _filter_players(squad_players, args.player)
            LOGGER.info(
                "Discovered %s %s players for season %s",
                len(squad_players),
                config.CLUB_NAME,
                season,
            )

            roster_payload = scraper.roster_scraper.build_roster_payload(season, squad_players)
            save_local_individual_json(
                data=roster_payload,
                source="transfermarkt",
                team=config.TEAM_KEY,
                artifact_name="team_roster",
                season=season,
                config=config,
            )
            roster_count += 1

            for index, player in enumerate(squad_players, start=1):
                payload = scraper.run_player(
                    player_url=player["player_url"],
                    season=season,
                    competition=args.competition,
                )
                save_local_individual_json(
                    data=payload,
                    source="transfermarkt",
                    team=config.TEAM_KEY,
                    artifact_name="player_detailed_stats_individual",
                    season=season,
                    entity=scraper.client.player_storage_key(
                        payload["player_name"],
                        payload["player_id"],
                    ),
                    config=config,
                )
                player_count += 1
                total_rows += len(payload["player_stats"])
                season_rows.extend(payload["player_stats"])
                LOGGER.info(
                    "Season %s player %s/%s: %s -> %s rows",
                    season,
                    index,
                    len(squad_players),
                    payload["player_name"],
                    len(payload["player_stats"]),
                )
            save_local_combined_csv(
                rows=season_rows,
                source="transfermarkt",
                team=config.TEAM_KEY,
                artifact_name="player_detailed_stats_combined",
                season=season,
                config=config,
            )

        print(f"Roster files written: {roster_count}")
        print(f"Players scraped: {player_count}")
        print(f"Total rows scraped: {total_rows}")
    except Exception as exc:
        LOGGER.exception("Top-level local scrape-all run failed: %s", exc)


if __name__ == "__main__":
    main()
