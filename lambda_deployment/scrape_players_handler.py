import os
import re
from typing import Any, Optional
from datetime import datetime, timezone

from src.loader.s3_loader import save_bronze_s3
from src.scraper.scrape_player import PlayerLogScraper
from src.scraper.scrape_roster import TeamRosterScraper
from utils.team_config import config_from_request


def _normalize_player_selector(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _filter_players(
    players: list[dict[str, str]],
    player_selector: Optional[str],
) -> list[dict[str, str]]:
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
    del context
    request = event or {}
    team = request.get("team", "manchester_united")
    season = request.get("season", "2025")
    player = request.get("player")          # now expects a dict from Map, not a name string
    competition = request.get("competition")
    scrape_date = request.get("scrape_date")

    bucket = os.environ["S3_BUCKET"]
    bronze_prefix = os.environ.get("S3_RAW_PREFIX", "raw")

    config = config_from_request(request, require_transfermarkt_identity=True)
    roster_scraper = TeamRosterScraper(config=config)
    player_scraper = PlayerLogScraper(config=config, roster_scraper=roster_scraper)

    # ← CHANGED: if a player object is passed directly (from Map), use it.
    # Falls back to full roster scrape for standalone/local invocations.
    if isinstance(player, dict) and "player_url" in player:
        squad_players = [player]
    else:
        squad_players = roster_scraper.get_squad_players(season)
        squad_players = _filter_players(squad_players, player)  # player is a name string here

    total_rows = 0
    player_keys: list[str] = []
    for squad_player in squad_players:
        payload = player_scraper.run_player(
            player_url=squad_player["player_url"],
            season=season,
            competition=competition,
        )
        key = save_bronze_s3(
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
            scrape_date=scrape_date,
        )
        player_keys.append(key)
        total_rows += len(payload["player_stats"])

    return {
        "statusCode": 200,
        "team": config.TEAM_KEY,
        "club": config.CLUB_NAME,
        "season": season,
        "players_scraped": len(squad_players),
        "total_rows": total_rows,
        "player_keys": player_keys,
    }
