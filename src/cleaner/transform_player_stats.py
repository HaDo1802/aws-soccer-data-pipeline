import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


class PlayerStatsTransformer:
    COLUMNS = [
        "season",
        "season_label",
        "club",
        "player_name",
        "player_id",
        "competition_code",
        "matchday",
        "match_date",
        "match_date_iso",
        "venue",
        "is_home_match",
        "home_team",
        "home_team_name",
        "home_team_rank",
        "away_team",
        "away_team_name",
        "away_team_rank",
        "result",
        "position",
        "goals",
        "assists",
        "own_goals",
        "yellow_cards",
        "second_yellow_red_cards",
        "red_cards",
        "subbed_on_minute",
        "subbed_off_minute",
        "performance_rating",
        "minutes_played",
        "note",
    ]

    NULLABLE_INT_COLUMNS = [
        "matchday",
        "home_team_rank",
        "away_team_rank",
        "goals",
        "assists",
        "own_goals",
        "yellow_cards",
        "second_yellow_red_cards",
        "red_cards",
        "subbed_on_minute",
        "subbed_off_minute",
        "minutes_played",
    ]

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()

    def transform_seasons(self, seasons: Optional[list[str]] = None) -> list[Path]:
        target_seasons = seasons or self.config.SEASONS
        written_paths: list[Path] = []
        for season in target_seasons:
            written_path = self.transform_season(season, team=self.config.TEAM_KEY)
            if written_path is not None:
                written_paths.append(written_path)
        return written_paths

    def transform_season(self, season: str, team: Optional[str] = None) -> Optional[Path]:
        active_team = team or self.config.TEAM_KEY
        bronze_root = (
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / active_team
            / "player_detailed_stats_combined"
        )
        season_files = sorted(bronze_root.glob(f"{season}/scrape_date=*.csv"))
        transformed_rows: list[dict[str, Any]] = []

        for path in season_files:
            raw_frame = pd.read_csv(path, dtype=object)
            transformed_rows.extend(
                self.transform_row(
                    self._clean_raw_row(row),
                    club=self.config.CLUB_NAME,
                    season_label=self.config.SEASON_LABELS.get(season, season),
                )
                for row in raw_frame.to_dict(orient="records")
            )

        if not transformed_rows:
            return None

        output_path = self._parquet_output_path(season, active_team)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dataframe = pd.DataFrame(transformed_rows, columns=self.COLUMNS)
        dataframe = self._apply_dtypes(dataframe)
        dataframe.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
        LOGGER.info("Wrote cleaned player stats parquet to %s", output_path)
        return output_path

    def transform_player_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        season = payload.get("season")
        season_label = payload.get("season_label") or self.config.SEASON_LABELS.get(season, season)
        rows = payload.get("player_stats", [])
        transformed_rows = [
            self.transform_row(
                row,
                club=payload.get("club"),
                season_label=season_label,
            )
            for row in rows
        ]
        return {
            "season": season,
            "team": payload.get("team", self.config.TEAM_KEY),
            "season_label": season_label,
            "club": payload.get("club"),
            "player_name": payload.get("player_name"),
            "player_id": payload.get("player_id"),
            "transformed_at": datetime.now(timezone.utc).isoformat(),
            "player_stats": transformed_rows,
        }

    def transform_row(
        self,
        row: dict[str, Any],
        club: Optional[str] = None,
        season_label: Optional[str] = None,
    ) -> dict[str, Any]:
        home_team_name, home_team_rank = self._split_team_and_rank(row.get("home_team"))
        away_team_name, away_team_rank = self._split_team_and_rank(row.get("away_team"))
        season = row.get("season")
        return {
            **row,
            "season_label": season_label or self.config.SEASON_LABELS.get(season, season),
            "club": club,
            "match_date_iso": self._normalize_date(row.get("match_date")),
            "home_team_name": home_team_name,
            "home_team_rank": home_team_rank,
            "away_team_name": away_team_name,
            "away_team_rank": away_team_rank,
            "is_home_match": self._infer_is_home(home_team_name, away_team_name),
        }

    def _parquet_output_path(self, season: str, team: str) -> Path:
        scrape_date = datetime.now(timezone.utc).date().isoformat()
        return (
            Path(self.config.LOCAL_SILVER_ROOT)
            / "transfermarkt"
            / team
            / "player_stats"
            / season
            / f"scrape_date={scrape_date}.parquet"
        )

    def _apply_dtypes(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        typed = dataframe.copy()
        for column in self.NULLABLE_INT_COLUMNS:
            typed[column] = typed[column].astype("Int64")
        typed["is_home_match"] = typed["is_home_match"].astype("boolean")
        typed["performance_rating"] = typed["performance_rating"].astype("float64")
        return typed

    def _clean_raw_row(self, row: dict[str, Any]) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        for key, value in row.items():
            if pd.isna(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        return cleaned

    def _normalize_date(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        cleaned = str(value).strip()
        if not cleaned:
            return None
        for date_format in ("%b %d, %Y", "%m/%d/%y", "%b %d, %y"):
            try:
                return datetime.strptime(cleaned, date_format).date().isoformat()
            except ValueError:
                continue
        return None

    def _split_team_and_rank(self, value: Any) -> tuple[Optional[str], Optional[int]]:
        if value is None:
            return None, None
        cleaned = str(value).strip()
        if not cleaned:
            return None, None
        match = re.match(r"^(.*?)\s*\((\d+)\.\)$", cleaned)
        if match:
            return match.group(1).strip(), int(match.group(2))
        return cleaned, None

    def _infer_is_home(self, home_team_name: Optional[str], away_team_name: Optional[str]) -> Optional[bool]:
        club = self.config.CLUB_NAME
        if home_team_name == club:
            return True
        if away_team_name == club:
            return False
        return None
