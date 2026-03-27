import os
from typing import Any, Optional

from src.cleaner.transform_player_stats import PlayerStatsTransformer
from src.loader.s3_loader import load_combined_bronze_csv_from_s3, save_silver_s3_csv
from utils.config import Config


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    team = request.get("team", "manchester_united")
    season = request.get("season", "2025")
    scrape_date = request.get("scrape_date")

    bucket = os.environ["S3_BUCKET"]
    bronze_prefix = os.environ.get("S3_BRONZE_PREFIX", "bronze")
    silver_prefix = os.environ.get("S3_SILVER_PREFIX", "silver")

    config = Config().for_team(team)
    transformer = PlayerStatsTransformer(config=config)

    bronze_rows, resolved_scrape_date, bronze_key = load_combined_bronze_csv_from_s3(
        team=config.TEAM_KEY,
        season=season,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        scrape_date=scrape_date,
    )

    transformed_rows = transformer.transform_rows(
        rows=(transformer.normalize_raw_row(row) for row in bronze_rows),
        season=season,
        club=config.CLUB_NAME,
    )
    silver_key = save_silver_s3_csv(
        rows=transformed_rows,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="player_stats",
        season=season,
        bucket=bucket,
        silver_prefix=silver_prefix,
        scrape_date=resolved_scrape_date,
    )

    return {
        "statusCode": 200,
        "team": config.TEAM_KEY,
        "club": config.CLUB_NAME,
        "season": season,
        "scrape_date": resolved_scrape_date,
        "source_bronze_key": bronze_key,
        "silver_key": silver_key,
        "rows_written": len(transformed_rows),
    }
