from typing import Any, Optional

from src.scraper.scrape_league import LeagueScraper


def handler(event: Optional[dict[str, Any]], context: Any) -> dict[str, Any]:
    del context
    request = event or {}
    league_id = request.get("league_id", "GB1")
    seasons = request.get("seasons") or ["2025"]

    scraper = LeagueScraper()
    teams_by_id: dict[str, dict[str, str]] = {}
    for season in seasons:
        season_teams = scraper.scrape_teams(league_id, season)
        for team in season_teams:
            teams_by_id.setdefault(team["club_id"], team)

    teams = list(teams_by_id.values())
    return {
        "statusCode": 200,
        "league_id": league_id,
        "seasons": seasons,
        "teams": teams,
        "teams_found": len(teams),
    }
