import unittest

from src.scraper.scrape_roster import TeamRosterScraper


class StubClient:
    def __init__(self, html: str) -> None:
        self.html = html

    def fetch(self, url: str) -> str:
        return self.html

    def extract_player_parts(self, player_url: str) -> tuple[str, str]:
        parts = [part for part in player_url.split("/") if part]
        player_slug = parts[-3]
        player_id = parts[-1]
        return player_slug, player_id

    def clean_player_anchor_text(self, value: str) -> str:
        return " ".join(value.split())

    def slug_to_name(self, slug: str) -> str:
        return slug.replace("-", " ").title()


class TeamRosterScraperTests(unittest.TestCase):
    def test_build_roster_url_uses_compact_kader_path(self) -> None:
        scraper = TeamRosterScraper(client=StubClient("<html></html>"))

        self.assertEqual(
            "https://www.transfermarkt.us/manchester-united/kader/verein/985/saison_id/2025/plus/1",
            scraper.build_roster_url("2025"),
        )

    def test_get_squad_players_extracts_unique_players(self) -> None:
        html = """
        <html>
          <body>
            <a href="/harry-maguire/profil/spieler/177907">Harry Maguire</a>
            <a href="/bruno-fernandes/profil/spieler/240306">Bruno Fernandes</a>
            <a href="/bruno-fernandes/profil/spieler/240306">Bruno Fernandes</a>
          </body>
        </html>
        """
        scraper = TeamRosterScraper(client=StubClient(html))

        players = scraper.get_squad_players("2025")

        self.assertEqual(2, len(players))
        self.assertEqual("177907", players[0]["player_id"])
        self.assertEqual("Harry Maguire", players[0]["player_name"])
        self.assertEqual("240306", players[1]["player_id"])

    def test_build_roster_payload_shapes_output(self) -> None:
        scraper = TeamRosterScraper(client=StubClient("<html></html>"))

        payload = scraper.build_roster_payload(
            "2025",
            [
                {
                    "player_name": "Bruno Fernandes",
                    "player_slug": "bruno-fernandes",
                    "player_id": "240306",
                    "player_url": "https://example.com/bruno-fernandes/profil/spieler/240306",
                }
            ],
        )

        self.assertEqual("2025", payload["season"])
        self.assertEqual("manchester_united", payload["team"])
        self.assertEqual("2025/2026", payload["season_label"])
        self.assertEqual("Manchester United", payload["club"])
        self.assertEqual("985", payload["club_id"])
        self.assertEqual(1, len(payload["players"]))


if __name__ == "__main__":
    unittest.main()
