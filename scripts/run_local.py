import argparse
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from typing import Any, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified local CLI for scraping, backfills, uploads, cleaning, and Snowflake ingest.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    league_parser = subparsers.add_parser(
        "league",
        help="Discover teams for one league across one or more seasons.",
    )
    league_parser.add_argument("--league-id", default="GB1")
    league_parser.add_argument("--seasons", nargs="+", default=["2025"])
    league_parser.set_defaults(func=run_league)

    team_parser = subparsers.add_parser(
        "team",
        help="Run the local team pipeline for one configured team or one explicitly provided club.",
    )
    team_parser.add_argument("--team", default=None)
    team_parser.add_argument("--club-name", default=None)
    team_parser.add_argument("--club-slug", default=None)
    team_parser.add_argument("--club-id", default=None)
    team_parser.add_argument("--season", default=None)
    team_parser.add_argument("--player", default=None)
    team_parser.add_argument("--competition", default=None)
    team_parser.add_argument("--scrape-date", default=None)
    team_parser.add_argument("--skip-clean", action="store_true")
    team_parser.set_defaults(func=run_team)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Run a league-wide local backfill, with optional S3 upload and Snowflake ingest.",
    )
    backfill_parser.add_argument("--league-id", default="GB1")
    backfill_parser.add_argument("--seasons", nargs="+", default=["2021", "2022", "2023", "2024", "2025"])
    backfill_parser.add_argument("--competition", default=None)
    backfill_parser.add_argument("--bucket", default=None)
    backfill_parser.add_argument("--skip-upload", action="store_true")
    backfill_parser.add_argument("--skip-ingest", action="store_true")
    backfill_parser.add_argument("--team-limit", type=int, default=None)
    backfill_parser.set_defaults(func=run_backfill)

    clean_parser = subparsers.add_parser(
        "clean",
        help="Transform local combined bronze CSVs into cleaned player stats CSVs.",
    )
    clean_parser.add_argument("--team", required=True)
    clean_parser.add_argument("--season", default=None)
    clean_parser.add_argument("--scrape-date", default=None)
    clean_parser.set_defaults(func=run_clean)

    upload_parser = subparsers.add_parser(
        "upload",
        help="Upload local raw and cleaned files to S3.",
    )
    upload_parser.add_argument("--team", required=True)
    upload_parser.add_argument("--season", default=None)
    upload_parser.add_argument("--bucket", default=None)
    upload_parser.add_argument("--bronze-prefix", default=None)
    upload_parser.add_argument("--silver-prefix", default=None)
    upload_parser.add_argument("--raw-only", action="store_true")
    upload_parser.add_argument("--dry-run", action="store_true")
    upload_parser.set_defaults(func=run_upload)

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Load one cleaned team-season snapshot into Snowflake.",
    )
    ingest_parser.add_argument("--team", required=True)
    ingest_parser.add_argument("--season", required=True)
    ingest_parser.add_argument("--scrape-date", required=True)
    ingest_parser.set_defaults(func=run_ingest)

    return parser


def run_league(args: argparse.Namespace) -> None:
    from src.scraper.scrape_league import LeagueScraper

    scraper = LeagueScraper()
    teams_by_id: dict[str, dict[str, str]] = {}
    for season in args.seasons:
        teams = scraper.scrape_teams(args.league_id, season)
        LOGGER.info(
            "Discovered %s teams for league %s in season %s",
            len(teams),
            args.league_id,
            season,
        )
        for team in teams:
            teams_by_id.setdefault(team["club_id"], team)

    print(f"League: {args.league_id}")
    print(f"Seasons: {', '.join(args.seasons)}")
    print(f"Teams found: {len(teams_by_id)}")
    for team in sorted(teams_by_id.values(), key=lambda item: item["club_name"]):
        print(f"- {team['club_name']} ({team['club_id']})")


def run_team(args: argparse.Namespace) -> None:
    scrape_date = args.scrape_date or _today()
    config = _resolve_cli_team_config(
        team=args.team,
        club_name=args.club_name,
        club_slug=args.club_slug,
        club_id=args.club_id,
    )
    seasons = [args.season] if args.season else config.SEASONS

    roster_count = 0
    player_count = 0
    total_rows = 0
    cleaned_count = 0
    for season in seasons:
        summary = _scrape_team_season(
            config=config,
            season=season,
            competition=args.competition,
            scrape_date=scrape_date,
            player_selector=args.player,
            include_cleaned=not args.skip_clean,
        )
        roster_count += 1
        player_count += summary["players_scraped"]
        total_rows += summary["rows_scraped"]
        cleaned_count += int(summary["cleaned"])

    print(f"Scrape date: {scrape_date}")
    print(f"Roster files written: {roster_count}")
    print(f"Players scraped: {player_count}")
    print(f"Total rows scraped: {total_rows}")
    print(f"Cleaned files written: {cleaned_count}")


def run_backfill(args: argparse.Namespace) -> None:
    from src.loader.local_bronze import save_local_individual_json
    from src.loader.s3_loader import S3Loader
    from src.scraper.scrape_league import LeagueScraper
    from src.loader import snowflake_loader

    scrape_date = _today()
    base_config = Config()
    competition_code = args.competition or args.league_id
    bucket = args.bucket or base_config.S3_BUCKET
    should_upload = not args.skip_upload
    should_ingest = not args.skip_ingest

    if should_ingest:
        load_dotenv()
        _required_env("SNOWFLAKE_ACCOUNT")
        _required_env("SNOWFLAKE_USER")
        _required_env("SNOWFLAKE_PASSWORD")
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
            team_config = _config_for_discovered_team(base_config, team)
            label = f"{team_config.TEAM_KEY}:{season}"
            try:
                summary = _scrape_team_season(
                    config=team_config,
                    season=season,
                    competition=competition_code,
                    scrape_date=scrape_date,
                    player_selector=None,
                    include_cleaned=True,
                )
                if s3_loader is not None:
                    _upload_files(s3_loader, summary["files"], bucket)
                if should_ingest:
                    result = snowflake_loader.ingest_season(
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


def run_clean(args: argparse.Namespace) -> None:
    from src.cleaner.transform_player_stats import PlayerStatsTransformer

    config = _resolve_local_team_config(args.team, args.season, args.scrape_date)
    seasons = [args.season] if args.season else config.SEASONS
    transformer = PlayerStatsTransformer(config=config)
    written_paths = transformer.transform_seasons(
        seasons=seasons,
        scrape_date=args.scrape_date,
    )
    print(f"Cleaned files written: {len(written_paths)}")


def run_upload(args: argparse.Namespace) -> None:
    from src.loader.s3_loader import S3Loader

    config = _resolve_local_team_config(args.team, args.season, scrape_date=None)
    config = replace(
        config,
        S3_RAW_PREFIX=args.bronze_prefix or config.S3_RAW_PREFIX,
        S3_CLEANED_PREFIX=args.silver_prefix or config.S3_CLEANED_PREFIX,
    )
    load_dotenv()
    loader = S3Loader(config=config)
    files = loader.collect_local_files(
        season=args.season,
        team=config.TEAM_KEY,
        include_cleaned=not args.raw_only,
    )
    uploaded_keys = loader.upload_files(
        files=files,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )
    print(f"Files processed: {len(uploaded_keys)}")


def run_ingest(args: argparse.Namespace) -> None:
    from src.loader import snowflake_loader

    load_dotenv()
    config = _resolve_local_team_config(args.team, args.season, args.scrape_date)
    os.environ["SNOWFLAKE_ACCOUNT"] = _required_env("SNOWFLAKE_ACCOUNT")
    os.environ["SNOWFLAKE_USER"] = _required_env("SNOWFLAKE_USER")
    os.environ["SNOWFLAKE_PASSWORD"] = _required_env("SNOWFLAKE_PASSWORD")
    os.environ["SNOWFLAKE_WAREHOUSE"] = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    result = snowflake_loader.ingest_season(
        team=config.TEAM_KEY,
        season=args.season,
        scrape_date=args.scrape_date,
        config=config,
    )
    print("Snowflake ingest completed successfully")
    print(f"Team: {result['team']}")
    print(f"Season: {result['season']}")
    print(f"Scrape date: {result['scrape_date']}")
    print(f"Rows staged: {result['rows_staged']}")
    print(f"Rows merged: {result['rows_merged']}")
    print(f"Rows in bronze: {result['rows_in_bronze']}")


def _scrape_team_season(
    config: Config,
    season: str,
    competition: Optional[str],
    scrape_date: str,
    player_selector: Optional[str],
    include_cleaned: bool,
) -> dict[str, object]:
    from src.cleaner.transform_player_stats import PlayerStatsTransformer
    from src.loader.local_bronze import save_local_combined_csv, save_local_individual_json
    from src.scraper.scrape_player import PlayerLogScraper

    scraper = PlayerLogScraper(config=config)
    squad_players = scraper.roster_scraper.get_squad_players(season)
    squad_players = _filter_players(squad_players, player_selector)
    LOGGER.info(
        "Discovered %s players for %s in season %s",
        len(squad_players),
        config.CLUB_NAME,
        season,
    )

    roster_payload = scraper.roster_scraper.build_roster_payload(season, squad_players)
    written_paths: list[Path] = [
        save_local_individual_json(
            data=roster_payload,
            source="transfermarkt",
            team=config.TEAM_KEY,
            artifact_name="team_roster",
            season=season,
            config=config,
            scrape_date=scrape_date,
        )
    ]

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

    cleaned = False
    if include_cleaned:
        cleaned_path = _clean_team_season(config, season, scrape_date, PlayerStatsTransformer)
        written_paths.append(cleaned_path)
        cleaned = True

    return {
        "files": written_paths,
        "players_scraped": len(squad_players),
        "rows_scraped": len(season_rows),
        "cleaned": cleaned,
    }


def _clean_team_season(
    config: Config,
    season: str,
    scrape_date: str,
    transformer_cls: Optional[type] = None,
) -> Path:
    if transformer_cls is None:
        from src.cleaner.transform_player_stats import PlayerStatsTransformer

        transformer_cls = PlayerStatsTransformer

    transformer = transformer_cls(config=config)
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


def _resolve_cli_team_config(
    team: Optional[str],
    club_name: Optional[str],
    club_slug: Optional[str],
    club_id: Optional[str],
) -> Config:
    base_config = Config()
    active_team = team or base_config.TEAM_KEY
    if active_team in base_config.TEAM_CONFIGS:
        return base_config.for_team(active_team)
    if all([club_name, club_slug, club_id]):
        return base_config.for_runtime_team(
            team_key=active_team,
            club_name=club_name,
            club_slug=club_slug,
            club_id=str(club_id),
        )
    raise ValueError(
        f"Unsupported team '{active_team}'. Provide --club-name, --club-slug, and --club-id, or use a configured team."
    )


def _resolve_local_team_config(team: str, season: Optional[str], scrape_date: Optional[str]) -> Config:
    base_config = Config()
    if team in base_config.TEAM_CONFIGS:
        return base_config.for_team(team)

    if not season or not scrape_date:
        raise ValueError(
            f"Unsupported team '{team}'. For dynamic teams, provide both --season and --scrape-date so roster metadata can be loaded."
        )

    roster_path = (
        Path(base_config.LOCAL_RAW_ROOT)
        / "transfermarkt"
        / team
        / "team_roster"
        / season
        / f"scrape_date={scrape_date}.json"
    )
    if not roster_path.is_file():
        raise ValueError(f"Unsupported team '{team}' and no roster payload found at {roster_path}")

    payload = json.loads(roster_path.read_text(encoding="utf-8"))
    club_name = payload.get("club")
    club_id = payload.get("club_id")
    if not club_name or not club_id:
        raise ValueError(f"Roster payload missing club metadata: {roster_path}")

    return base_config.for_runtime_team(
        team_key=team,
        club_name=club_name,
        club_slug=_team_key_to_slug(team),
        club_id=str(club_id),
    )


def _config_for_discovered_team(base_config: Config, team: dict[str, str]) -> Config:
    return base_config.for_runtime_team(
        team_key=team["team_key"],
        club_name=team["club_name"],
        club_slug=team["club_slug"],
        club_id=team["club_id"],
    )


def _normalize_player_selector(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _filter_players(players: list[dict[str, str]], player_selector: Optional[str]) -> list[dict[str, str]]:
    if not player_selector:
        return players

    normalized_selector = _normalize_player_selector(player_selector)
    return [
        player
        for player in players
        if _normalize_player_selector(player["player_name"]) == normalized_selector
        or _normalize_player_selector(player["player_slug"]) == normalized_selector
    ]


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _upload_files(s3_loader: Any, files: list[Path], bucket: str) -> None:
    uploaded_keys = s3_loader.upload_files(files=files, bucket=bucket)
    LOGGER.info("Uploaded %s files to s3://%s", len(uploaded_keys), bucket)


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _team_key_to_slug(team_key: str) -> str:
    return team_key.replace("_", "-")


if __name__ == "__main__":
    main()
