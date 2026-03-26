import re
import time
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.runtime import Config, get_logger


class TransfermarktClient:
    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.logger = get_logger(__name__)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": self.config.TRANSFERMARKT_BASE_URL,
            }
        )

    def fetch(self, url: str) -> str:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            time.sleep(self.config.REQUEST_DELAY_SECONDS)
            try:
                response = self.session.get(url, timeout=30)
                self.logger.info("GET %s -> %s", url, response.status_code)
                response.raise_for_status()
                return response.text
            except requests.RequestException as exc:
                last_error = exc
                self.logger.warning(
                    "Request failed for %s on attempt %s/%s: %s",
                    url,
                    attempt,
                    self.config.MAX_RETRIES,
                    exc,
                )

        raise RuntimeError(
            f"Failed to fetch {url} after {self.config.MAX_RETRIES} attempts"
        ) from last_error

    def extract_player_parts(self, player_url: str) -> tuple[str, str]:
        path_parts = [part for part in urlparse(player_url).path.split("/") if part]
        if "spieler" not in path_parts:
            raise ValueError(f"Could not parse player id from URL: {player_url}")
        spieler_index = path_parts.index("spieler")
        if len(path_parts) < 3 or spieler_index + 1 >= len(path_parts):
            raise ValueError(f"Could not parse player slug from URL: {player_url}")
        return path_parts[0], path_parts[spieler_index + 1]

    def slug_to_name(self, slug: str) -> str:
        return " ".join(part.capitalize() for part in slug.split("-"))

    def clean_value(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def clean_player_anchor_text(self, value: str) -> str:
        cleaned = self.clean_value(value)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    def extract_player_name_from_html(self, html: str) -> Optional[str]:
        soup = BeautifulSoup(html, "lxml")
        heading = soup.find("h1")
        if heading is None:
            return None
        heading_text = heading.get_text(" ", strip=True)
        heading_text = re.sub(r"^#\d+\s+", "", heading_text).strip()
        return heading_text or None

    def player_storage_key(self, player_name: str, player_id: str) -> str:
        normalized_name = re.sub(r"[^a-z0-9]+", "-", player_name.lower()).strip("-")
        if normalized_name:
            return f"player_id={player_id}_{normalized_name}"
        return f"player_id={player_id}"
