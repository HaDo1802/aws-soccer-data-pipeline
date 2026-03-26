import unittest

from src.cleaner.transform_player_stats import PlayerStatsTransformer


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


if __name__ == "__main__":
    unittest.main()
