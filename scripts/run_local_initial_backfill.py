import argparse
from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from typing import Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.cleaner.transform_player_stats import PlayerStatsTransformer
from src.loader.local_bronze import save_local_combined_csv, save_local_individual_json
from src.loader.s3_loader import S3Loader
from src.loader.snowflake_loader import ingest_season
from src.scraper.scrape_league import LeagueScraper
from src.scraper.scrape_player import PlayerLogScraper
from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--league-id", default="GB1")
    parser.add_argument("--seasons", nargs="+", default=["2021", "2022", "2023", "2024", "2025"])
    parser.add_argument("--competition", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument("--team-limit", type=int, default=None)
    args = parser.parse_args()

    scrape_date = datetime.now(timezone.utc).date().isoformat()
    base_config = Config()
    competition_code = args.competition or args.league_id
    bucket = args.bucket or base_config.S3_BUCKET
    should_upload = not args.skip_upload
    should_ingest = not args.skip_ingest

    if should_ingest:
        load_dotenv()
        _require_env("SNOWFLAKE_ACCOUNT")
        _require_env("SNOWFLAKE_USER")
        _require_env("SNOWFLAKE_PASSWORD")
    elif should_upload:
        load_dotenv()

    league_scraper = LeagueScraper(config=base_config)
    season_teams: dict[str, list[dict[str, str]]] = {}
    league_team_key = league_scraper.LEAGUE_SLUGS.get(args.league_id, args.league_id.lower()).replace("-", "_")
    s3_loader = S3Loader(config=base_config) if should_upload else None

    for season in args.seasons:
        teams = league_scraper.scrape_teams(args.league_id, season)
        if args.team_limit is not None:
            teams = teams[: args.team_limit]
        season_teams[season] = teams
        payload = league_scraper.build_league_payload(args.league_id, season, teams)
        league_path = save_local_individual_json(
            data=payload,
            source="transfermarkt",
            team=league_team_key,
            artifact_name="league_teams",
            season=season,
            config=base_config,
            scrape_date=scrape_date,
        )
        if s3_loader is not None:
            _upload_files(s3_loader, [league_path], bucket)
        LOGGER.info(
            "Saved %s discovered teams for league %s season %s",
            len(teams),
            args.league_id,
            season,
        )

    successes: list[str] = []
    failures: list[str] = []
    for season in args.seasons:
        for team in season_teams[season]:
            team_config = _config_for_team(base_config, team)
            label = f"{team_config.TEAM_KEY}:{season}"
            try:
                generated_files = _scrape_team_season(team_config, season, competition_code, scrape_date)
                cleaned_path = _clean_team_season(team_config, season, scrape_date)
                generated_files.append(cleaned_path)
                if s3_loader is not None:
                    _upload_files(s3_loader, generated_files, bucket)
                if should_ingest:
                    result = ingest_season(
                        team=team_config.TEAM_KEY,
                        season=season,
                        scrape_date=scrape_date,
                        config=team_config,
                    )
                    LOGGER.info(
                        "Snowflake ingest complete for %s season %s: merged=%s bronze=%s",
                        result["team"],
                        result["season"],
                        result["rows_merged"],
                        result["rows_in_bronze"],
                    )
                successes.append(label)
            except Exception as exc:
                failures.append(label)
                LOGGER.exception("Backfill failed for %s: %s", label, exc)

    print(f"Scrape date: {scrape_date}")
    print(f"Completed team-seasons: {len(successes)}")
    print(f"Failed team-seasons: {len(failures)}")
    if failures:
        print("Failures:")
        for label in failures:
            print(f"- {label}")


def _scrape_team_season(
    config: Config,
    season: str,
    competition: Optional[str],
    scrape_date: str,
) -> list[Path]:
    scraper = PlayerLogScraper(config=config)
    squad_players = scraper.roster_scraper.get_squad_players(season)
    LOGGER.info(
        "Discovered %s players for %s in season %s",
        len(squad_players),
        config.CLUB_NAME,
        season,
    )

    roster_payload = scraper.roster_scraper.build_roster_payload(season, squad_players)
    written_paths: list[Path] = []
    written_paths.append(
        save_local_individual_json(
            data=roster_payload,
            source="transfermarkt",
            team=config.TEAM_KEY,
            artifact_name="team_roster",
            season=season,
            config=config,
            scrape_date=scrape_date,
        )
    )

    season_rows: list[dict[str, object]] = []
    for index, player in enumerate(squad_players, start=1):
        payload = scraper.run_player(
            player_url=player["player_url"],
            season=season,
            competition=competition,
        )
        written_paths.append(
            save_local_individual_json(
                data=payload,
                source="transfermarkt",
                team=config.TEAM_KEY,
                artifact_name="player_detailed_stats_individual",
                season=season,
                entity=scraper.client.player_storage_key(payload["player_name"], payload["player_id"]),
                config=config,
                scrape_date=scrape_date,
            )
        )
        season_rows.extend(payload["player_stats"])
        LOGGER.info(
            "Season %s %s player %s/%s: %s -> %s rows",
            season,
            config.TEAM_KEY,
            index,
            len(squad_players),
            payload["player_name"],
            len(payload["player_stats"]),
        )

    combined_path = save_local_combined_csv(
        rows=season_rows,
        source="transfermarkt",
        team=config.TEAM_KEY,
        artifact_name="player_detailed_stats_combined",
        season=season,
        config=config,
        scrape_date=scrape_date,
    )
    if combined_path is not None:
        written_paths.append(combined_path)
    return written_paths


def _clean_team_season(config: Config, season: str, scrape_date: str) -> Path:
    transformer = PlayerStatsTransformer(config=config)
    output_path = transformer.transform_season(
        season=season,
        team=config.TEAM_KEY,
        scrape_date=scrape_date,
    )
    if output_path is None:
        raise ValueError(
            f"No cleaned output produced for team={config.TEAM_KEY} season={season} scrape_date={scrape_date}"
        )
    LOGGER.info("Cleaned player stats written to %s", output_path)
    return output_path


def _config_for_team(base_config: Config, team: dict[str, str]) -> Config:
    return replace(
        base_config,
        TEAM_KEY=team["team_key"],
        CLUB_NAME=team["club_name"],
        TRANSFERMARKT_CLUB_SLUG=team["club_slug"],
        TRANSFERMARKT_CLUB_ID=team["club_id"],
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _upload_files(s3_loader: S3Loader, files: list[Path], bucket: str) -> None:
    uploaded_keys = s3_loader.upload_files(files=files, bucket=bucket)
    LOGGER.info("Uploaded %s files to s3://%s", len(uploaded_keys), bucket)


if __name__ == "__main__":
    main()
