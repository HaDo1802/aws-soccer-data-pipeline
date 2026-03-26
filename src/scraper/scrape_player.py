import re
from datetime import datetime, timezone
from typing import Any, Optional

from bs4 import BeautifulSoup

from src.scraper.scrape_roster import TeamRosterScraper
from src.scraper.transfermarkt_client import TransfermarktClient
from utils.config import Config
from utils.logger import get_logger


class PlayerLogScraper:
    def __init__(
        self,
        config: Optional[Config] = None,
        client: Optional[TransfermarktClient] = None,
        roster_scraper: Optional[TeamRosterScraper] = None,
    ) -> None:
        self.config = config or Config()
        self.client = client or TransfermarktClient(config=self.config)
        self.roster_scraper = roster_scraper or TeamRosterScraper(
            config=self.config,
            client=self.client,
        )
        self.logger = get_logger(__name__)

    def build_player_stats_url(
        self,
        player_slug: str,
        player_id: str,
        season: str,
        competition_code: str,
    ) -> str:
        return (
            f"{self.config.TRANSFERMARKT_BASE_URL}/{player_slug}/leistungsdatendetails/"
            f"spieler/{player_id}/saison/{season}/verein/0/liga/0/"
            f"wettbewerb/{competition_code}/pos/0/trainer_id/0/plus/1"
        )

    def scrape_player_season(
        self,
        player_name: str,
        player_id: str,
        player_slug: str,
        season: str,
        competition: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        competition_code = competition or self.config.TRANSFERMARKT_DEFAULT_COMPETITION
        url = self.build_player_stats_url(
            player_slug=player_slug,
            player_id=player_id,
            season=season,
            competition_code=competition_code,
        )
        try:
            html = self.client.fetch(url)
        except RuntimeError as exc:
            self.logger.warning(
                "Skipping player %s (%s) for %s after fetch failure: %s",
                player_name,
                player_id,
                season,
                exc,
            )
            return []

        resolved_player_name = self.client.extract_player_name_from_html(html) or player_name
        try:
            return self.parse(
                html=html,
                player_name=resolved_player_name,
                player_id=player_id,
                season=season,
                competition=competition_code,
            )
        except ValueError as exc:
            self.logger.warning(
                "Skipping player %s (%s) for %s after parse failure: %s",
                resolved_player_name,
                player_id,
                season,
                exc,
            )
            return []

    def run_player(
        self,
        player_url: str,
        season: str,
        competition: Optional[str] = None,
    ) -> dict[str, Any]:
        player_slug, player_id = self.client.extract_player_parts(player_url)
        fallback_player_name = self.client.slug_to_name(player_slug)
        rows = self.scrape_player_season(
            player_name=fallback_player_name,
            player_id=player_id,
            player_slug=player_slug,
            season=season,
            competition=competition,
        )
        resolved_player_name = rows[0]["player_name"] if rows else fallback_player_name
        return self.build_player_payload(
            season=season,
            player_name=resolved_player_name,
            player_id=player_id,
            rows=rows,
        )

    def build_player_payload(
        self,
        season: str,
        player_name: str,
        player_id: str,
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "team": self.config.TEAM_KEY,
            "season": season,
            "season_label": self.config.SEASON_LABELS.get(season, season),
            "club": self.config.CLUB_NAME,
            "player_name": player_name,
            "player_id": player_id,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "player_stats": rows,
        }

    def parse(
        self,
        html: str,
        player_name: str,
        player_id: str,
        season: str,
        competition: str,
    ) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        target_table = self._find_match_table(soup)
        if target_table is None:
            raise ValueError("Could not locate Transfermarkt detailed stats table in HTML")

        records: list[dict[str, Any]] = []
        for row in target_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            values = [cell.get_text(" ", strip=True) for cell in cells]
            matchday = self._to_int(values[0])
            match_date = self.client.clean_value(values[1])
            if not matchday or not self._looks_like_match_date(match_date):
                continue

            if self._is_status_row(values):
                records.append(
                    self._build_status_row(
                        values=values,
                        player_name=player_name,
                        player_id=player_id,
                        season=season,
                        competition=competition,
                        matchday=matchday,
                        match_date=match_date,
                    )
                )
                continue

            result_index = self._find_result_index(values)
            if result_index is None:
                continue

            core_match = self._extract_match_context(values, result_index)
            stat_values = self._extract_stat_values(values, result_index)
            stat_slice = self._normalize_stat_values(stat_values)

            records.append(
                {
                    "player_name": player_name,
                    "player_id": player_id,
                    "season": season,
                    "competition_code": competition,
                    "matchday": matchday,
                    "match_date": match_date,
                    "venue": core_match["venue"],
                    "home_team": core_match["home_team"],
                    "away_team": core_match["away_team"],
                    "result": core_match["result"],
                    "position": core_match["position"],
                    "goals": self._parse_stat_number(stat_slice["goals"]),
                    "assists": self._parse_stat_number(stat_slice["assists"]),
                    "own_goals": self._parse_stat_number(stat_slice["own_goals"]),
                    "yellow_cards": self._parse_stat_number(stat_slice["yellow_cards"]),
                    "second_yellow_red_cards": self._parse_stat_number(stat_slice["second_yellow_red_cards"]),
                    "red_cards": self._parse_stat_number(stat_slice["red_cards"]),
                    "subbed_on_minute": self._parse_minutes(stat_slice["subbed_on_minute"]),
                    "subbed_off_minute": self._parse_minutes(stat_slice["subbed_off_minute"]),
                    "performance_rating": self._to_float(stat_slice["performance_rating"]),
                    "minutes_played": self._parse_minutes(stat_slice["minutes_played"]),
                    "note": stat_slice["note"],
                }
            )
        return records

    def _build_status_row(
        self,
        values: list[str],
        player_name: str,
        player_id: str,
        season: str,
        competition: str,
        matchday: int,
        match_date: str,
    ) -> dict[str, Any]:
        venue = None
        if len(values) > 2 and self.client.clean_value(values[2]) in {"H", "A"}:
            venue = self.client.clean_value(values[2])
        return {
            "player_name": player_name,
            "player_id": player_id,
            "season": season,
            "competition_code": competition,
            "matchday": matchday,
            "match_date": match_date,
            "venue": venue,
            "home_team": None,
            "away_team": None,
            "result": None,
            "position": None,
            "goals": None,
            "assists": None,
            "own_goals": None,
            "yellow_cards": None,
            "second_yellow_red_cards": None,
            "red_cards": None,
            "subbed_on_minute": None,
            "subbed_off_minute": None,
            "performance_rating": None,
            "minutes_played": None,
            "note": self._extract_status_note(values),
        }

    def _extract_match_context(self, values: list[str], result_index: int) -> dict[str, Optional[str]]:
        pre_result_values = [
            self.client.clean_value(value)
            for value in values[2:result_index]
            if self.client.clean_value(value)
        ]
        venue = (
            pre_result_values[0]
            if pre_result_values and pre_result_values[0] in {"H", "A"}
            else None
        )
        team_values = pre_result_values[1:] if venue else pre_result_values
        if len(team_values) >= 2:
            home_team = team_values[-2]
            away_team = team_values[-1]
        else:
            home_team = None
            away_team = None

        position_index = result_index + 1 if result_index + 1 < len(values) else None
        position = self.client.clean_value(values[position_index]) if position_index is not None else None

        return {
            "venue": venue,
            "home_team": home_team,
            "away_team": away_team,
            "result": self.client.clean_value(values[result_index]),
            "position": position,
        }

    def _extract_stat_values(self, values: list[str], result_index: int) -> list[str]:
        position_index = result_index + 1 if result_index + 1 < len(values) else None
        if position_index is None:
            return []
        return [self.client.clean_value(value) for value in values[position_index + 1 :]]

    def _find_match_table(self, soup: BeautifulSoup):
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            matched_rows = 0
            for row in rows[:8]:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue
                values = [cell.get_text(" ", strip=True) for cell in cells]
                if self._to_int(values[0]) is not None and self._looks_like_match_date(values[1]):
                    matched_rows += 1
            if matched_rows >= 2:
                return table
        return None

    def _looks_like_match_date(self, value: str) -> bool:
        if not value:
            return False
        for date_format in ("%m/%d/%y", "%b %d, %Y", "%b %d, %y"):
            try:
                datetime.strptime(value, date_format)
                return True
            except ValueError:
                continue
        return False

    def _to_int(self, value: str) -> Optional[int]:
        if not value:
            return None
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return int(float(cleaned))
        except ValueError:
            return None

    def _parse_minutes(self, value: Any) -> Optional[int]:
        cleaned = self.client.clean_value(value).replace("'", "")
        return self._to_int(cleaned)

    def _to_float(self, value: str) -> Optional[float]:
        if not value:
            return None
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _find_result_index(self, values: list[str]) -> Optional[int]:
        for index, value in enumerate(values):
            if re.fullmatch(r"\d+:\d+", self.client.clean_value(value)):
                return index
        return None

    def _extract_note_from_stats(self, values: list[str]) -> Optional[str]:
        note_parts = [self.client.clean_value(value) for value in values if self.client.clean_value(value)]
        if not note_parts:
            return None
        return " ".join(note_parts)

    def _normalize_stat_values(self, stat_values: list[str]) -> dict[str, Optional[str]]:
        stat_values = [self.client.clean_value(value) for value in stat_values]
        core = {
            "goals": None,
            "assists": None,
            "own_goals": None,
            "yellow_cards": None,
            "second_yellow_red_cards": None,
            "red_cards": None,
            "subbed_on_minute": None,
            "subbed_off_minute": None,
            "performance_rating": None,
            "minutes_played": None,
            "note": None,
        }

        if not stat_values:
            return core

        padded = stat_values + [""] * max(0, 10 - len(stat_values))
        core["goals"] = padded[0] or None
        core["assists"] = padded[1] or None
        core["own_goals"] = padded[2] or None
        core["yellow_cards"] = padded[3] or None
        core["second_yellow_red_cards"] = padded[4] or None
        core["red_cards"] = padded[5] or None
        core["subbed_on_minute"] = padded[6] or None
        core["subbed_off_minute"] = padded[7] or None
        core["performance_rating"] = padded[8] or None
        core["minutes_played"] = padded[9] or None
        core["note"] = self._extract_note_from_stats(stat_values[10:])
        return core

    def _looks_like_minutes(self, value: str) -> bool:
        cleaned = self.client.clean_value(value)
        return bool(cleaned and re.fullmatch(r"\d+'", cleaned))

    def _parse_stat_number(self, value: Optional[str]) -> Optional[int]:
        cleaned = self.client.clean_value(value)
        if not cleaned:
            return None
        if self._looks_like_minutes(cleaned):
            return self._parse_minutes(cleaned)
        return self._to_int(cleaned)

    def _is_status_row(self, values: list[str]) -> bool:
        if len(values) < 6:
            return False
        venue_marker = self.client.clean_value(values[2]) in {"H", "A"}
        missing_core_match_shape = not self.client.clean_value(values[3]) or not self.client.clean_value(values[5])
        trailing_text = any(
            keyword in " ".join(values).lower()
            for keyword in ("injury", "problems", "absence", "suspended", "calf", "thigh", "ankle")
        )
        return venue_marker and missing_core_match_shape and trailing_text

    def _extract_status_note(self, values: list[str]) -> Optional[str]:
        note_candidates = [
            self.client.clean_value(value)
            for value in values[6:]
            if self.client.clean_value(value)
        ]
        if not note_candidates:
            return None
        return " ".join(note_candidates)
