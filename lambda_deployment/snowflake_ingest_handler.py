from typing import Any, Optional

from src.loader import snowflake_loader
from utils.config import Config
from utils.team_config import config_from_request


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    base_config = Config()
    team = request.get("team", base_config.TEAM_KEY)
    season = request.get("season", base_config.SEASONS[-1])
    scrape_date = request.get("scrape_date")
    if not scrape_date:
        raise ValueError("scrape_date is required")

    config = config_from_request(request, default_team=base_config.TEAM_KEY)
    result = snowflake_loader.ingest_season(
        team=config.TEAM_KEY,
        season=season,
        scrape_date=scrape_date,
        config=config,
    )
    return {
        "statusCode": 200,
        **result,
    }
