from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.runtime import Config, get_logger
from src.scraper.transfermarkt_client import TransfermarktClient


class TeamRosterScraper:
    def __init__(
        self,
        config: Optional[Config] = None,
        client: Optional[TransfermarktClient] = None,
    ) -> None:
        self.config = config or Config()
        self.client = client or TransfermarktClient(config=self.config)
        self.logger = get_logger(__name__)

    def build_roster_url(self, season: str) -> str:
        return (
            f"{self.config.TRANSFERMARKT_BASE_URL}/"
            f"{self.config.TRANSFERMARKT_CLUB_SLUG}/kader/verein/"
            f"{self.config.TRANSFERMARKT_CLUB_ID}/saison_id/{season}/plus/1"
        )

    def get_squad_players(self, season: str) -> list[dict[str, str]]:
        html = self.client.fetch(self.build_roster_url(season))
        soup = BeautifulSoup(html, "lxml")
        players: list[dict[str, str]] = []
        seen_ids: set[str] = set()

        for anchor in soup.select('a[href*="/profil/spieler/"]'):
            href = anchor.get("href")
            if not href:
                continue
            full_url = urljoin(self.config.TRANSFERMARKT_BASE_URL, href)
            try:
                player_slug, player_id = self.client.extract_player_parts(full_url)
            except ValueError:
                continue
            if player_id in seen_ids:
                continue

            player_name = self.client.clean_player_anchor_text(anchor.get_text(" ", strip=True))
            if not player_name:
                player_name = self.client.slug_to_name(player_slug)

            players.append(
                {
                    "player_name": player_name,
                    "player_slug": player_slug,
                    "player_id": player_id,
                    "player_url": full_url,
                }
            )
            seen_ids.add(player_id)

        return players

    def build_roster_payload(self, season: str, players: list[dict[str, str]]) -> dict[str, Any]:
        return {
            "team": self.config.TEAM_KEY,
            "season": season,
            "season_label": self.config.SEASON_LABELS.get(season, season),
            "club": self.config.CLUB_NAME,
            "club_id": self.config.TRANSFERMARKT_CLUB_ID,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "players": players,
        }
