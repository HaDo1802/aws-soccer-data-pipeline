import logging
from dataclasses import dataclass, field, replace


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logger.addHandler(stream_handler)
    return logger


@dataclass(frozen=True)
class Config:
    TEAM_KEY: str = "manchester_united"
    CLUB_NAME: str = "Manchester United"
    TRANSFERMARKT_CLUB_SLUG: str = "manchester-united"
    TRANSFERMARKT_CLUB_ID: str = "985"
    TRANSFERMARKT_BASE_URL: str = "https://www.transfermarkt.us"
    TRANSFERMARKT_DEFAULT_COMPETITION: str = "GB1"
    SEASON_LABELS: dict[str, str] = field(
        default_factory=lambda: {
            "2021": "2021/2022",
            "2022": "2022/2023",
            "2023": "2023/2024",
            "2024": "2024/2025",
            "2025": "2025/2026",
        }
    )
    TEAM_CONFIGS: dict[str, dict[str, str]] = field(
        default_factory=lambda: {
            "manchester_united": {
                "club_name": "Manchester United",
                "club_slug": "manchester-united",
                "club_id": "985",
            },
        }
    )
    REQUEST_DELAY_SECONDS: int = 4
    MAX_RETRIES: int = 3

    def for_team(self, team_key: str) -> "Config":
        if team_key not in self.TEAM_CONFIGS:
            raise ValueError(
                f"Unsupported team '{team_key}'. Available teams: {', '.join(sorted(self.TEAM_CONFIGS))}"
            )

        team_config = self.TEAM_CONFIGS[team_key]
        return replace(
            self,
            TEAM_KEY=team_key,
            CLUB_NAME=team_config["club_name"],
            TRANSFERMARKT_CLUB_SLUG=team_config["club_slug"],
            TRANSFERMARKT_CLUB_ID=team_config["club_id"],
        )
