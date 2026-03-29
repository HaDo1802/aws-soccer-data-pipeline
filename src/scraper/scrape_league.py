import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.scraper.transfermarkt_client import TransfermarktClient
from utils.config import Config
from utils.logger import get_logger


class LeagueScraper:
    LEAGUE_SLUGS: dict[str, str] = {
        "GB1": "premier-league",
    }

    def __init__(
        self,
        config: Optional[Config] = None,
        client: Optional[TransfermarktClient] = None,
    ) -> None:
        self.config = config or Config()
        self.client = client or TransfermarktClient(config=self.config)
        self.logger = get_logger(__name__)

    def build_league_url(self, league_id: str, season: str) -> str:
        league_slug = self.LEAGUE_SLUGS.get(league_id, league_id.lower())
        return (
            f"{self.config.TRANSFERMARKT_BASE_URL}/{league_slug}/startseite/"
            f"wettbewerb/{league_id}/plus/?saison_id={season}"
        )

    def scrape_teams(self, league_id: str, season: str) -> list[dict[str, str]]:
        html = self.client.fetch(self.build_league_url(league_id, season))
        soup = BeautifulSoup(html, "lxml")
        teams: list[dict[str, str]] = []
        seen_ids: set[str] = set()

        for anchor in soup.select('a[href*="/verein/"]'):
            href = anchor.get("href")
            if not href:
                continue

            full_url = urljoin(self.config.TRANSFERMARKT_BASE_URL, href)
            try:
                club_slug, club_id = self.extract_club_parts(full_url)
            except ValueError:
                continue

            if club_id in seen_ids:
                continue

            club_name = self.client.clean_player_anchor_text(anchor.get_text(" ", strip=True))
            if not club_name:
                club_name = self.client.slug_to_name(club_slug)

            teams.append(
                {
                    "team_key": self.club_name_to_team_key(club_name),
                    "club_name": club_name,
                    "club_slug": club_slug,
                    "club_id": club_id,
                    "team_url": full_url,
                }
            )
            seen_ids.add(club_id)

        return teams

    def build_league_payload(
        self,
        league_id: str,
        season: str,
        teams: list[dict[str, str]],
    ) -> dict[str, Any]:
        return {
            "league_id": league_id,
            "season": season,
            "season_label": self.config.SEASON_LABELS.get(season, season),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "teams": teams,
            "teams_found": len(teams),
        }

    def extract_club_parts(self, club_url: str) -> tuple[str, str]:
        path_parts = [part for part in urlparse(club_url).path.split("/") if part]
        if "verein" not in path_parts:
            raise ValueError(f"Could not parse club id from URL: {club_url}")
        verein_index = path_parts.index("verein")
        if verein_index == 0 or verein_index + 1 >= len(path_parts):
            raise ValueError(f"Could not parse club slug from URL: {club_url}")
        return path_parts[0], path_parts[verein_index + 1]

    def club_name_to_team_key(self, club_name: str) -> str:
        normalized_name = re.sub(r"[^a-z0-9]+", "_", club_name.lower())
        return normalized_name.strip("_")
