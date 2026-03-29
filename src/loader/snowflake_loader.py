import os
from pathlib import Path
from typing import Any, Optional

import snowflake.connector

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)
MINIMUM_EXPECTED_ROWS = 50
STAGING_TABLE = "SOCCER_ANALYTICS.STAGING.PLAYER_STATS_RAW"
BRONZE_TABLE = "SOCCER_ANALYTICS.BRONZE.PLAYER_STATS"


def ingest_season(
    team: str,
    season: str,
    scrape_date: str,
    config: Optional[Config] = None,
) -> dict[str, Any]:
    if not scrape_date:
        raise ValueError("scrape_date is required")

    base_config = config or Config()
    if team == base_config.TEAM_KEY:
        active_config = base_config
    else:
        active_config = base_config.for_team(team)
    connection = None
    try:
        connection = connect_snowflake(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )

        rows_staged = _copy_into_staging(connection, team, season, scrape_date)
        if rows_staged < MINIMUM_EXPECTED_ROWS:
            raise ValueError(
                f"Staged rows below minimum threshold: {rows_staged} < {MINIMUM_EXPECTED_ROWS}"
            )

        rows_merged = _merge_into_bronze(connection, active_config.TEAM_KEY, season, scrape_date)
        _cleanup_staging(connection)
        rows_in_bronze = _count_rows_in_bronze(connection, active_config.TEAM_KEY, season)

        return {
            "team": active_config.TEAM_KEY,
            "season": season,
            "scrape_date": scrape_date,
            "rows_staged": rows_staged,
            "rows_merged": rows_merged,
            "rows_in_bronze": rows_in_bronze,
        }
    finally:
        if connection is not None:
            connection.close()


def _copy_into_staging(connection: Any, team: str, season: str, scrape_date: str) -> int:
    copy_sql = _load_sql(
        "staging/copy_into_staging.sql",
        team=team,
        season=season,
        scrape_date=scrape_date,
    )

    with connection.cursor() as cursor:
        LOGGER.info("Running Snowflake COPY INTO for team=%s season=%s scrape_date=%s", team, season, scrape_date)
        cursor.execute(copy_sql)
        return _extract_copy_rows(cursor, cursor.fetchall())


def _merge_into_bronze(connection: Any, team: str, season: str, scrape_date: str) -> int:
    merge_sql = _load_sql(
        "bronze/merge_into_bronze.sql",
        team=team,
        season=season,
        scrape_date=scrape_date,
    )

    with connection.cursor() as cursor:
        LOGGER.info("Running Snowflake MERGE INTO for team=%s season=%s scrape_date=%s", team, season, scrape_date)
        cursor.execute(merge_sql)
        return _extract_merge_rows(cursor, cursor.fetchall())


def _count_rows_in_bronze(connection: Any, team: str, season: str) -> int:
    query = _load_sql(
        "bronze/count_bronze_rows.sql",
        team=team,
        season=season,
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            return 0

        columns = [description[0].lower() for description in cursor.description or []]
        if "rows_in_bronze" in columns:
            return int(row[columns.index("rows_in_bronze")] or 0)

        return int(row[0])


def _cleanup_staging(connection: Any) -> None:
    cleanup_sql = _load_sql("staging/cleanup_staging.sql")
    with connection.cursor() as cursor:
        LOGGER.info("Running Snowflake staging cleanup")
        cursor.execute(cleanup_sql)


def _load_sql(relative_path: str, **params: str) -> str:
    sql_root = Path(__file__).resolve().parents[2] / "sql" / "snowflake"
    sql_path = sql_root / relative_path
    return sql_path.read_text(encoding="utf-8").format(**params)


def _extract_copy_rows(cursor: Any, copy_results: list[Any]) -> int:
    if not copy_results:
        return 0

    columns = [description[0].lower() for description in cursor.description or []]
    if "rows_loaded" in columns:
        index = columns.index("rows_loaded")
        return sum(int(row[index] or 0) for row in copy_results)

    if len(copy_results[0]) > 3:
        return sum(int(row[3] or 0) for row in copy_results)

    return 0


def _extract_merge_rows(cursor: Any, merge_results: list[Any]) -> int:
    if not merge_results:
        return 0

    columns = [description[0].lower() for description in cursor.description or []]
    inserted = 0
    updated = 0

    if "rows_inserted" in columns:
        inserted_index = columns.index("rows_inserted")
        inserted = sum(int(row[inserted_index] or 0) for row in merge_results)
    if "rows_updated" in columns:
        updated_index = columns.index("rows_updated")
        updated = sum(int(row[updated_index] or 0) for row in merge_results)

    if inserted or updated:
        return inserted + updated

    if len(merge_results[0]) >= 2:
        return sum(int(row[0] or 0) + int(row[1] or 0) for row in merge_results)

    return 0


def connect_snowflake(
    account: str,
    user: str,
    password: str,
    warehouse: str = "COMPUTE_WH",
) -> Any:
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
    )
