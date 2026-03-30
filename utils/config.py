from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Config:
    TEAM_KEY: str = "manchester_united"
    CLUB_NAME: str = "Manchester United"
    TRANSFERMARKT_CLUB_SLUG: str = "manchester-united"
    TRANSFERMARKT_CLUB_ID: str = "985"
    TRANSFERMARKT_BASE_URL: str = "https://www.transfermarkt.us"
    TRANSFERMARKT_DEFAULT_COMPETITION: str = "GB1"
    SEASONS: list[str] = field(
        default_factory=lambda: [
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
        ]
    )
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
    LOCAL_RAW_ROOT: str = "data/raw"
    LOCAL_CLEANED_ROOT: str = "data/cleaned"
    S3_BUCKET: str = "sport-analysis"
    S3_RAW_PREFIX: str = "raw"
    S3_CLEANED_PREFIX: str = "cleaned"
    REQUEST_DELAY_SECONDS: int = 1
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

    def for_runtime_team(
        self,
        team_key: str,
        club_name: str,
        club_slug: str,
        club_id: str,
    ) -> "Config":
        return replace(
            self,
            TEAM_KEY=team_key,
            CLUB_NAME=club_name,
            TRANSFERMARKT_CLUB_SLUG=club_slug,
            TRANSFERMARKT_CLUB_ID=club_id,
        )
