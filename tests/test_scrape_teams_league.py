import unittest
from unittest.mock import patch

from lambda_deployment.scrape_teams_league_handler import handler
from src.scraper.scrape_league import LeagueScraper


class StubClient:
    def __init__(self, html_by_url: dict[str, str]) -> None:
        self.html_by_url = html_by_url

    def fetch(self, url: str) -> str:
        return self.html_by_url[url]

    def clean_value(self, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def clean_player_anchor_text(self, value: str) -> str:
        return " ".join(value.split())

    def slug_to_name(self, slug: str) -> str:
        return slug.replace("-", " ").title()


class LeagueScraperTests(unittest.TestCase):
    def test_scrape_teams_extracts_unique_clubs(self) -> None:
        html = """
        <html>
          <body>
            <a title="Manchester United" href="/manchester-united/startseite/verein/985/saison_id/2025">Manchester United</a>
            <a title="Arsenal FC" href="/arsenal-fc/startseite/verein/11/saison_id/2025">Arsenal FC</a>
            <a title="Arsenal FC" href="/arsenal-fc/startseite/verein/11/saison_id/2025">Arsenal FC</a>
            <a title="Rasenballsport Leipzig" href="/rasenballsport-leipzig/startseite/verein/23826/saison_id/2024">Rasenballsport Leipzig</a>
          </body>
        </html>
        """
        scraper = LeagueScraper(
            client=StubClient(
                {
                    "https://www.transfermarkt.us/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=2025": html,
                }
            )
        )

        teams = scraper.scrape_teams("GB1", "2025")

        self.assertEqual(2, len(teams))
        self.assertEqual("manchester_united", teams[0]["team_key"])
        self.assertEqual("Manchester United", teams[0]["club_name"])
        self.assertEqual("manchester-united", teams[0]["club_slug"])
        self.assertEqual("985", teams[0]["club_id"])
        self.assertEqual("arsenal-fc", teams[1]["club_slug"])
        self.assertEqual("11", teams[1]["club_id"])

    def test_build_league_payload_shapes_output(self) -> None:
        scraper = LeagueScraper(client=StubClient({}))

        payload = scraper.build_league_payload(
            "GB1",
            "2025",
            [
                {
                    "team_key": "manchester_united",
                    "club_name": "Manchester United",
                    "club_slug": "manchester-united",
                    "club_id": "985",
                }
            ],
        )

        self.assertEqual("GB1", payload["league_id"])
        self.assertEqual("2025", payload["season"])
        self.assertEqual("2025/2026", payload["season_label"])
        self.assertEqual(1, payload["teams_found"])
        self.assertEqual(1, len(payload["teams"]))


class LeagueHandlerTests(unittest.TestCase):
    @patch("lambda_deployment.scrape_teams_league_handler.LeagueScraper")
    def test_handler_deduplicates_teams_across_seasons(self, scraper_cls: unittest.mock.Mock) -> None:
        scraper = scraper_cls.return_value
        scraper.scrape_teams.side_effect = [
            [
                {
                    "team_key": "manchester_united",
                    "club_name": "Manchester United",
                    "club_slug": "manchester-united",
                    "club_id": "985",
                },
                {
                    "team_key": "arsenal_fc",
                    "club_name": "Arsenal FC",
                    "club_slug": "arsenal-fc",
                    "club_id": "11",
                },
            ],
            [
                {
                    "team_key": "arsenal_fc",
                    "club_name": "Arsenal FC",
                    "club_slug": "arsenal-fc",
                    "club_id": "11",
                }
            ],
        ]

        result = handler(
            {
                "league_id": "GB1",
                "seasons": ["2025", "2024"],
            },
            None,
        )

        self.assertEqual(200, result["statusCode"])
        self.assertEqual("GB1", result["league_id"])
        self.assertEqual(["2025", "2024"], result["seasons"])
        self.assertEqual(2, result["teams_found"])
        self.assertEqual(["985", "11"], [team["club_id"] for team in result["teams"]])


if __name__ == "__main__":
    unittest.main()
