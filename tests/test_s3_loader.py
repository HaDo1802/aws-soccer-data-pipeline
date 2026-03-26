import tempfile
import unittest
from pathlib import Path

from src.loader.s3_loader import S3Loader
from utils.config import Config


class S3LoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.config = Config(
            LOCAL_BRONZE_ROOT=str(self.temp_path / "data" / "bronze"),
            LOCAL_SILVER_ROOT=str(self.temp_path / "data" / "silver"),
            S3_BRONZE_PREFIX="bronze",
            S3_SILVER_PREFIX="silver",
        )
        self.loader = S3Loader(config=self.config)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_build_s3_key_routes_bronze_and_silver_files(self) -> None:
        bronze_file = (
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "team_roster"
            / "2025"
            / "roster.json"
        )
        cleaned_file = (
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "player_detailed_stats_combined"
            / "2025"
            / "stats.csv"
        )
        silver_file = (
            Path(self.config.LOCAL_SILVER_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "player_stats"
            / "2025"
            / "scrape_date=2026-03-23.parquet"
        )

        self.assertEqual(
            "bronze/transfermarkt/manchester_united/team_roster/2025/roster.json",
            self.loader.build_s3_key(bronze_file),
        )
        self.assertEqual(
            "bronze/transfermarkt/manchester_united/player_detailed_stats_combined/2025/stats.csv",
            self.loader.build_s3_key(cleaned_file),
        )
        self.assertEqual(
            "silver/transfermarkt/manchester_united/player_stats/2025/scrape_date=2026-03-23.parquet",
            self.loader.build_s3_key(silver_file),
        )

    def test_collect_local_files_filters_by_season_and_includes_silver(self) -> None:
        paths = [
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "team_roster"
            / "2025"
            / "roster.json",
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "player_detailed_stats_individual"
            / "2024"
            / "player.json",
            Path(self.config.LOCAL_BRONZE_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "player_detailed_stats_combined"
            / "2025"
            / "player.csv",
            Path(self.config.LOCAL_SILVER_ROOT)
            / "transfermarkt"
            / "manchester_united"
            / "player_stats"
            / "2025"
            / "scrape_date=2026-03-23.parquet",
        ]

        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")

        files = self.loader.collect_local_files(
            season="2025",
            team="manchester_united",
            include_cleaned=True,
        )

        self.assertEqual(
            [
                Path(self.config.LOCAL_BRONZE_ROOT)
                / "transfermarkt"
                / "manchester_united"
                / "player_detailed_stats_combined"
                / "2025"
                / "player.csv",
                Path(self.config.LOCAL_BRONZE_ROOT)
                / "transfermarkt"
                / "manchester_united"
                / "team_roster"
                / "2025"
                / "roster.json",
                Path(self.config.LOCAL_SILVER_ROOT)
                / "transfermarkt"
                / "manchester_united"
                / "player_stats"
                / "2025"
                / "scrape_date=2026-03-23.parquet",
            ],
            files,
        )


if __name__ == "__main__":
    unittest.main()
