import os
from typing import Any, Optional
from datetime import datetime, timezone

from src.loader.s3_loader import save_bronze_s3
from src.scraper.scrape_roster import TeamRosterScraper
from utils.config import Config


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    team = request.get("team", "manchester_united")
    season = request.get("season", "2025")
    scrape_date = datetime.now(timezone.utc).date().isoformat()

    bucket = os.environ["S3_BUCKET"]
    bronze_prefix = os.environ.get("S3_RAW_PREFIX", "raw")

    config = Config().for_team(team)
    scraper = TeamRosterScraper(config=config)
    players = scraper.get_squad_players(season)
    payload = scraper.build_roster_payload(season, players)
    key = save_bronze_s3(
        data=payload,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="team_roster",
        season=season,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        scrape_date=scrape_date,
    )

    return {
        "statusCode": 200,
        "team": config.TEAM_KEY,
        "club": config.CLUB_NAME,
        "season": season,
        "scrape_date": scrape_date,
        "players": players,
        "players_found": len(players),
        "roster_key": key,
    }
