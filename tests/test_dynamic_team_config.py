import os
import unittest
from unittest.mock import patch

from lambda_deployment.scrape_roster_handler import handler as scrape_roster_handler
from lambda_deployment.snowflake_ingest_handler import handler as snowflake_ingest_handler
from utils.team_config import config_from_request


class DynamicTeamConfigTests(unittest.TestCase):
    def test_config_from_request_supports_runtime_team_metadata(self) -> None:
        config = config_from_request(
            {
                "team": "arsenal_fc",
                "club_name": "Arsenal FC",
                "club_slug": "arsenal-fc",
                "club_id": "11",
            }
        )

        self.assertEqual("arsenal_fc", config.TEAM_KEY)
        self.assertEqual("Arsenal FC", config.CLUB_NAME)
        self.assertEqual("arsenal-fc", config.TRANSFERMARKT_CLUB_SLUG)
        self.assertEqual("11", config.TRANSFERMARKT_CLUB_ID)

    def test_config_from_request_requires_transfermarkt_identity_for_scrapers(self) -> None:
        with self.assertRaisesRegex(ValueError, "Provide club_name, club_slug, and club_id"):
            config_from_request(
                {"team": "arsenal_fc"},
                require_transfermarkt_identity=True,
            )

    @patch.dict(os.environ, {"S3_BUCKET": "test-bucket", "S3_RAW_PREFIX": "raw"}, clear=False)
    @patch("lambda_deployment.scrape_roster_handler.save_bronze_s3")
    @patch("lambda_deployment.scrape_roster_handler.TeamRosterScraper")
    def test_scrape_roster_handler_accepts_dynamic_team_metadata(
        self,
        scraper_cls: unittest.mock.Mock,
        save_bronze_s3: unittest.mock.Mock,
    ) -> None:
        scraper = scraper_cls.return_value
        scraper.get_squad_players.return_value = [
            {
                "player_name": "Bukayo Saka",
                "player_slug": "bukayo-saka",
                "player_id": "433177",
                "player_url": "https://example.com/bukayo-saka/profil/spieler/433177",
            }
        ]
        scraper.build_roster_payload.return_value = {"players": scraper.get_squad_players.return_value}
        save_bronze_s3.return_value = "raw/transfermarkt/arsenal_fc/team_roster/2025/scrape_date=2026-03-30.json"

        result = scrape_roster_handler(
            {
                "team": "arsenal_fc",
                "club_name": "Arsenal FC",
                "club_slug": "arsenal-fc",
                "club_id": "11",
                "season": "2025",
            },
            None,
        )

        self.assertEqual(200, result["statusCode"])
        self.assertEqual("arsenal_fc", result["team"])
        self.assertEqual("Arsenal FC", result["club"])
        scraper_cls.assert_called_once()
        self.assertEqual("arsenal-fc", scraper_cls.call_args.kwargs["config"].TRANSFERMARKT_CLUB_SLUG)
        self.assertEqual("11", scraper_cls.call_args.kwargs["config"].TRANSFERMARKT_CLUB_ID)

    @patch("lambda_deployment.snowflake_ingest_handler.snowflake_loader.ingest_season")
    def test_snowflake_ingest_handler_accepts_dynamic_team_metadata(
        self,
        ingest_season: unittest.mock.Mock,
    ) -> None:
        ingest_season.return_value = {
            "team": "arsenal_fc",
            "season": "2025",
            "scrape_date": "2026-03-30",
            "rows_staged": 100,
            "rows_merged": 100,
            "rows_in_bronze": 100,
        }

        result = snowflake_ingest_handler(
            {
                "team": "arsenal_fc",
                "club_name": "Arsenal FC",
                "club_slug": "arsenal-fc",
                "club_id": "11",
                "season": "2025",
                "scrape_date": "2026-03-30",
            },
            None,
        )

        self.assertEqual(200, result["statusCode"])
        self.assertEqual("arsenal_fc", result["team"])
        ingest_season.assert_called_once()
        passed_config = ingest_season.call_args.kwargs["config"]
        self.assertEqual("arsenal_fc", passed_config.TEAM_KEY)
        self.assertEqual("Arsenal FC", passed_config.CLUB_NAME)
        self.assertEqual("arsenal-fc", passed_config.TRANSFERMARKT_CLUB_SLUG)
        self.assertEqual("11", passed_config.TRANSFERMARKT_CLUB_ID)


if __name__ == "__main__":
    unittest.main()
