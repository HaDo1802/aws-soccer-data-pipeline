from dataclasses import replace
import os
from pathlib import Path
from typing import Any, Optional

import boto3

from src.cleaner.transform_player_stats import PlayerStatsTransformer
from utils.config import Config


def _resolve_scrape_date(
    s3_client: Any,
    bucket: str,
    bronze_prefix: str,
    team: str,
    season: str,
) -> str:
    prefix = "/".join([bronze_prefix, "transfermarkt", team, "player_detailed_stats_combined", season]) + "/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No bronze CSV found for team={team}, season={season}")
    latest = sorted(files)[-1]
    return Path(latest).stem.replace("scrape_date=", "")


def _clean_season(
    s3_client: Any,
    team: str,
    season: str,
    scrape_date: str,
    bucket: str,
    bronze_prefix: str,
    silver_prefix: str,
) -> dict[str, str]:
    bronze_key = "/".join(
        [bronze_prefix, "transfermarkt", team, "player_detailed_stats_combined", season, f"scrape_date={scrape_date}.csv"]
    )
    silver_key = "/".join(
        [silver_prefix, "transfermarkt", team, "player_stats", season, f"scrape_date={scrape_date}.csv"]
    )

    local_bronze_root = Path("/tmp/data/bronze")
    local_silver_root = Path("/tmp/data/silver")
    local_bronze_path = (
        local_bronze_root
        / "transfermarkt"
        / team
        / "player_detailed_stats_combined"
        / season
        / f"scrape_date={scrape_date}.csv"
    )
    local_bronze_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket, bronze_key, str(local_bronze_path))

    team_config = Config().for_team(team)
    config = replace(
        team_config,
        LOCAL_RAW_ROOT=str(local_bronze_root),
        LOCAL_CLEANED_ROOT=str(local_silver_root),
    )
    transformer = PlayerStatsTransformer(config=config)
    local_silver_path = transformer.transform_season(
        season=season,
        team=config.TEAM_KEY,
        scrape_date=scrape_date,
    )
    if local_silver_path is None:
        raise FileNotFoundError(
            f"No silver file produced for team={team}, season={season}, scrape_date={scrape_date}"
        )

    s3_client.upload_file(str(local_silver_path), bucket, silver_key)
    return {
        "team": team,
        "season": season,
        "scrape_date": scrape_date,
        "silver_key": silver_key,
    }


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    team = request.get("team", "manchester_united")
    scrape_date = request.get("scrape_date")

    bucket = os.environ.get("S3_BUCKET", "sport-analysis")
    bronze_prefix = os.environ.get("S3_RAW_PREFIX", "raw")
    silver_prefix = os.environ.get("S3_CLEANED_PREFIX", "cleaned")

    s3_client = boto3.client("s3")

    seasons = request.get("seasons") or [request.get("season", "2025")]

    results = []
    for season in seasons:
        resolved_date = scrape_date or _resolve_scrape_date(s3_client, bucket, bronze_prefix, team, season)
        result = _clean_season(s3_client, team, season, resolved_date, bucket, bronze_prefix, silver_prefix)
        results.append(result)

    return {
        "statusCode": 200,
        "seasons_cleaned": len(results),
        "results": results,
    }
