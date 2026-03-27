import os
import unittest
from unittest.mock import patch

from lambda_deployment.clean_player_stats_handler import handler


class CleanPlayerStatsHandlerTests(unittest.TestCase):
    @patch("lambda_deployment.clean_player_stats_handler.save_silver_s3_csv")
    @patch("lambda_deployment.clean_player_stats_handler.load_combined_bronze_csv_from_s3")
    def test_handler_transforms_bronze_snapshot_into_silver(self, mock_load_combined, mock_save_silver) -> None:
        os.environ["S3_BUCKET"] = "sport-analysis"
        os.environ["S3_BRONZE_PREFIX"] = "bronze"
        os.environ["S3_SILVER_PREFIX"] = "silver"

        mock_load_combined.return_value = (
            [
                {
                    "season": "2025",
                    "player_name": "Bruno Fernandes",
                    "player_id": "240306",
                    "competition_code": "GB1",
                    "matchday": "3",
                    "match_date": "Sep 13, 2025",
                    "venue": "",
                    "home_team": "Leverkusen (6.)",
                    "away_team": "Manchester United (1.)",
                    "result": "1:2",
                    "position": "AM",
                    "goals": "1",
                    "assists": "1",
                    "own_goals": "",
                    "yellow_cards": "",
                    "second_yellow_red_cards": "",
                    "red_cards": "",
                    "subbed_on_minute": "",
                    "subbed_off_minute": "88",
                    "performance_rating": "1.2",
                    "minutes_played": "88",
                    "note": "",
                }
            ],
            "2026-03-27",
            "bronze/transfermarkt/manchester_united/player_detailed_stats_combined/2025/scrape_date=2026-03-27.csv",
        )
        mock_save_silver.return_value = (
            "silver/transfermarkt/manchester_united/player_stats/2025/scrape_date=2026-03-27.csv"
        )

        result = handler(
            {
                "team": "manchester_united",
                "season": "2025",
            },
            None,
        )

        self.assertEqual(200, result["statusCode"])
        self.assertEqual("2026-03-27", result["scrape_date"])
        self.assertEqual(1, result["rows_written"])
        self.assertEqual(
            "silver/transfermarkt/manchester_united/player_stats/2025/scrape_date=2026-03-27.csv",
            result["silver_key"],
        )

        mock_load_combined.assert_called_once()
        mock_save_silver.assert_called_once()


if __name__ == "__main__":
    unittest.main()
