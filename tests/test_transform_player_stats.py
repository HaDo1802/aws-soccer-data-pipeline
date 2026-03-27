import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.cleaner.transform_player_stats import PlayerStatsTransformer
from utils.config import Config


class PlayerStatsTransformerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transformer = PlayerStatsTransformer()

    def test_transform_row_splits_team_rank_and_normalizes_date(self) -> None:
        row = {
            "season": "2025",
            "player_name": "Bruno Fernandes",
            "player_id": "240306",
            "competition_code": "GB1",
            "matchday": 3,
            "match_date": "Sep 13, 2025",
            "venue": None,
            "home_team": "Leverkusen (6.)",
            "away_team": "Manchester United (1.)",
            "result": "1:2",
            "position": "AM",
            "goals": 1,
            "assists": 1,
            "own_goals": None,
            "yellow_cards": None,
            "second_yellow_red_cards": None,
            "red_cards": None,
            "subbed_on_minute": None,
            "subbed_off_minute": 88,
            "performance_rating": 1.2,
            "minutes_played": 88,
            "note": None,
        }

        transformed = self.transformer.transform_row(row, club="Manchester United")

        self.assertEqual("2025/2026", transformed["season_label"])
        self.assertEqual("2025-09-13", transformed["match_date_iso"])
        self.assertEqual("Leverkusen", transformed["home_team_name"])
        self.assertEqual(6, transformed["home_team_rank"])
        self.assertEqual("Manchester United", transformed["away_team_name"])
        self.assertEqual(1, transformed["away_team_rank"])
        self.assertFalse(transformed["is_home_match"])

    def test_transform_season_uses_latest_bronze_snapshot_by_default(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = Config(
                LOCAL_BRONZE_ROOT=f"{temp_dir}/bronze",
                LOCAL_SILVER_ROOT=f"{temp_dir}/silver",
            )
            transformer = PlayerStatsTransformer(config=config)
            season_dir = (
                Path(config.LOCAL_BRONZE_ROOT)
                / "transfermarkt"
                / config.TEAM_KEY
                / "player_detailed_stats_combined"
                / "2025"
            )
            season_dir.mkdir(parents=True, exist_ok=True)
            self._write_combined_csv(
                season_dir / "scrape_date=2026-03-26.csv",
                player_name="Older Snapshot",
            )
            self._write_combined_csv(
                season_dir / "scrape_date=2026-03-27.csv",
                player_name="Latest Snapshot",
            )

            written_path = transformer.transform_season("2025")

            self.assertIsNotNone(written_path)
            assert written_path is not None
            self.assertEqual("scrape_date=2026-03-27.csv", written_path.name)
            contents = written_path.read_text(encoding="utf-8")
            self.assertIn("Latest Snapshot", contents)
            self.assertNotIn("Older Snapshot", contents)

    def test_transform_season_honors_requested_scrape_date(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = Config(
                LOCAL_BRONZE_ROOT=f"{temp_dir}/bronze",
                LOCAL_SILVER_ROOT=f"{temp_dir}/silver",
            )
            transformer = PlayerStatsTransformer(config=config)
            season_dir = (
                Path(config.LOCAL_BRONZE_ROOT)
                / "transfermarkt"
                / config.TEAM_KEY
                / "player_detailed_stats_combined"
                / "2025"
            )
            season_dir.mkdir(parents=True, exist_ok=True)
            self._write_combined_csv(
                season_dir / "scrape_date=2026-03-26.csv",
                player_name="Requested Snapshot",
            )

            written_path = transformer.transform_season("2025", scrape_date="2026-03-26")

            self.assertIsNotNone(written_path)
            assert written_path is not None
            self.assertEqual("scrape_date=2026-03-26.csv", written_path.name)
            self.assertIn("Requested Snapshot", written_path.read_text(encoding="utf-8"))

    def _write_combined_csv(self, path: Path, player_name: str) -> None:
        path.write_text(
            "\n".join(
                [
                    (
                        "season,player_name,player_id,competition_code,matchday,match_date,"
                        "venue,home_team,away_team,result,position,goals,assists,own_goals,"
                        "yellow_cards,second_yellow_red_cards,red_cards,subbed_on_minute,"
                        "subbed_off_minute,performance_rating,minutes_played,note"
                    ),
                    (
                        f"2025,{player_name},240306,GB1,3,\"Sep 13, 2025\",,"
                        "\"Leverkusen (6.)\",\"Manchester United (1.)\",1:2,AM,1,1,,,,,," 
                        "88,1.2,88,"
                    ),
                ]
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
