import os
from typing import Any, Optional

from src.loader.s3_loader import load_player_payloads_from_s3, resolve_scrape_date, save_bronze_s3_csv
from utils.config import Config


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    team = request.get("team", "manchester_united")
    season = request.get("season", "2025")
    scrape_date = request.get("scrape_date")

    bucket = os.environ["S3_BUCKET"]
    bronze_prefix = os.environ.get("S3_RAW_PREFIX", "raw")

    config = Config().for_team(team)
    payloads = load_player_payloads_from_s3(
        team=config.TEAM_KEY,
        season=season,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        scrape_date=scrape_date,
    )

    season_rows: list[dict[str, Any]] = []
    for payload in payloads:
        season_rows.extend(payload.get("player_stats", []))

    combined_key = save_bronze_s3_csv(
        rows=season_rows,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="player_detailed_stats_combined",
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
        "scrape_date": resolve_scrape_date(scrape_date),
        "player_payloads_found": len(payloads),
        "total_rows": len(season_rows),
        "combined_csv_key": combined_key,
    }
