import unittest

from src.scraper.scrape_player import PlayerLogScraper


class StubClient:
    def clean_value(self, value):
        if value is None:
            return ""
        return str(value).strip()

    def extract_player_name_from_html(self, html: str):
        return None

    def fetch(self, url: str) -> str:
        raise NotImplementedError

    def extract_player_parts(self, player_url: str):
        raise NotImplementedError

    def slug_to_name(self, slug: str) -> str:
        return slug.replace("-", " ").title()

    def player_storage_key(self, player_name: str, player_id: str) -> str:
        return f"player_id={player_id}_test-player"


class PlayerLogScraperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = PlayerLogScraper(client=StubClient())

    def test_build_player_payload_includes_season_label(self) -> None:
        payload = self.scraper.build_player_payload(
            season="2025",
            player_name="Bruno Fernandes",
            player_id="240306",
            rows=[],
        )

        self.assertEqual("2025", payload["season"])
        self.assertEqual("manchester_united", payload["team"])
        self.assertEqual("2025/2026", payload["season_label"])
        self.assertEqual("Bruno Fernandes", payload["player_name"])

    def test_parse_regular_match_row(self) -> None:
        html = """
        <html>
          <body>
            <table>
              <tbody>
                <tr>
                  <td>1</td>
                  <td>Aug 22, 2025</td>
                  <td>Bayern Munich</td>
                  <td>Leipzig</td>
                  <td>6:0</td>
                  <td>CF</td>
                  <td>3</td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td>86'</td>
                  <td>1.1</td>
                  <td>86'</td>
                </tr>
                <tr>
                  <td>2</td>
                  <td>Aug 30, 2025</td>
                  <td>Augsburg (4.)</td>
                  <td>Bayern Munich (1.)</td>
                  <td>2:3</td>
                  <td>CF</td>
                  <td></td>
                  <td>2</td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td>1.7</td>
                  <td>90'</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

        rows = self.scraper.parse(
            html=html,
            player_name="Harry Kane",
            player_id="132098",
            season="2025",
            competition="GB1",
        )

        self.assertEqual(2, len(rows))
        self.assertEqual("Bayern Munich", rows[0]["home_team"])
        self.assertEqual("Leipzig", rows[0]["away_team"])
        self.assertEqual("6:0", rows[0]["result"])
        self.assertEqual(3, rows[0]["goals"])
        self.assertEqual(86, rows[0]["subbed_off_minute"])
        self.assertEqual(1.1, rows[0]["performance_rating"])
        self.assertEqual(86, rows[0]["minutes_played"])

    def test_parse_status_row_keeps_note(self) -> None:
        html = """
        <html>
          <body>
            <table>
              <tbody>
                <tr>
                  <td>1</td>
                  <td>Mar 6, 2026</td>
                  <td>H</td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td>Calf problems</td>
                </tr>
                <tr>
                  <td>2</td>
                  <td>Mar 14, 2026</td>
                  <td>Leverkusen (6.)</td>
                  <td>Bayern Munich (1.)</td>
                  <td>1:1</td>
                  <td>CF</td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td></td>
                  <td>61'</td>
                  <td></td>
                  <td>2.8</td>
                  <td>29'</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

        rows = self.scraper.parse(
            html=html,
            player_name="Harry Kane",
            player_id="132098",
            season="2025",
            competition="GB1",
        )

        self.assertEqual("Calf problems", rows[0]["note"])
        self.assertIsNone(rows[0]["home_team"])
        self.assertEqual(61, rows[1]["subbed_on_minute"])
        self.assertEqual(29, rows[1]["minutes_played"])


if __name__ == "__main__":
    unittest.main()
