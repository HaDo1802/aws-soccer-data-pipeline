import os
import re
from typing import Any, Optional

from src.scraper.scrape_player import PlayerLogScraper
from src.scraper.scrape_roster import TeamRosterScraper
from src.loader.s3_bronze import save_bronze_s3, save_bronze_s3_csv
from src.runtime import Config


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


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    request = event or {}
    team = request.get("team", "manchester_united")
    season = request.get("season", "2025")
    player = request.get("player")
    competition = request.get("competition")

    bucket = os.environ["S3_BUCKET"]
    bronze_prefix = os.environ.get("S3_BRONZE_PREFIX", "bronze")

    base_config = Config()
    config = base_config.for_team(team)
    roster_scraper = TeamRosterScraper(config=config)
    player_scraper = PlayerLogScraper(config=config, roster_scraper=roster_scraper)

    squad_players = roster_scraper.get_squad_players(season)
    squad_players = _filter_players(squad_players, player)
    roster_payload = roster_scraper.build_roster_payload(season, squad_players)
    save_bronze_s3(
        data=roster_payload,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="team_roster",
        season=season,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
    )

    total_rows = 0
    season_rows = []
    for player in squad_players:
        payload = player_scraper.run_player(
            player_url=player["player_url"],
            season=season,
            competition=competition,
        )
        save_bronze_s3(
            data=payload,
            source="transfermarkt",
            team=config.TEAM_KEY,
            artifact_name="player_detailed_stats_individual",
            season=season,
            bucket=bucket,
            bronze_prefix=bronze_prefix,
            entity=player_scraper.client.player_storage_key(
                payload["player_name"],
                payload["player_id"],
            ),
        )
        total_rows += len(payload["player_stats"])
        season_rows.extend(payload["player_stats"])

    save_bronze_s3_csv(
        rows=season_rows,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="player_detailed_stats_combined",
        season=season,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
    )

    return {
        "statusCode": 200,
        "team": config.TEAM_KEY,
        "club": config.CLUB_NAME,
        "season": season,
        "players_scraped": len(squad_players),
        "total_rows": total_rows,
    }
