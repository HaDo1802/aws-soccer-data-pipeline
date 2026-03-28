import csv
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import boto3

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)
SCRAPE_DATE_KEY_PATTERN = re.compile(r"scrape_date=(\d{4}-\d{2}-\d{2})\.csv$")


def resolve_scrape_date(scrape_date: Optional[str] = None) -> str:
    if scrape_date:
        return scrape_date
    return datetime.now(timezone.utc).date().isoformat()


def save_bronze_s3(
    data: dict,
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "raw",
    entity: Optional[str] = None,
    scrape_date: Optional[str] = None,
) -> str:
    key_parts = [bronze_prefix, source, team, artifact_name]
    if entity is not None:
        key_parts.append(entity)
    key_parts.extend([season, f"scrape_date={resolve_scrape_date(scrape_date)}.json"])
    key = "/".join(key_parts)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    LOGGER.info("Wrote bronze data to s3://%s/%s", bucket, key)
    return key


def save_bronze_s3_csv(
    rows: list[dict[str, Any]],
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "raw",
    scrape_date: Optional[str] = None,
) -> Optional[str]:
    if not rows:
        return None

    resolved_scrape_date = resolve_scrape_date(scrape_date)
    scraped_at = datetime.now(timezone.utc).isoformat()
    key = "/".join(
        [
            bronze_prefix,
            source,
            team,
            artifact_name,
            season,
            f"scrape_date={resolved_scrape_date}.csv",
        ]
    )

    enriched_rows = [
        {
            **row,
            "scrape_date": resolved_scrape_date,
            "scraped_at": scraped_at,
        }
        for row in rows
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(enriched_rows[0].keys()))
    writer.writeheader()
    writer.writerows(enriched_rows)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    LOGGER.info("Wrote bronze CSV data to s3://%s/%s", bucket, key)
    return key


def load_player_payloads_from_s3(
    team: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "raw",
    scrape_date: Optional[str] = None,
    source: str = "transfermarkt",
) -> list[dict[str, Any]]:
    s3_client = boto3.client("s3")
    target_scrape_date = resolve_scrape_date(scrape_date)
    prefix = "/".join([bronze_prefix, source, team, "player_detailed_stats_individual"])
    suffix = f"/{season}/scrape_date={target_scrape_date}.json"

    payloads: list[dict[str, Any]] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(suffix):
                continue
            response = s3_client.get_object(Bucket=bucket, Key=key)
            payloads.append(json.loads(response["Body"].read().decode("utf-8")))

    return payloads


def load_combined_bronze_csv_from_s3(
    team: str,
    season: str,
    bucket: str,
    bronze_prefix: str = "raw",
    scrape_date: Optional[str] = None,
    source: str = "transfermarkt",
) -> tuple[list[dict[str, Any]], str, str]:
    s3_client = boto3.client("s3")
    prefix = "/".join([bronze_prefix, source, team, "player_detailed_stats_combined", season]) + "/"

    if scrape_date is not None:
        key = prefix + f"scrape_date={scrape_date}.csv"
    else:
        key = _latest_s3_key_for_prefix(
            bucket=bucket,
            prefix=prefix,
            suffix=".csv",
        )
        if key is None:
            raise FileNotFoundError(
                f"No combined bronze CSV found at s3://{bucket}/{prefix}"
            )

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(body))
    resolved_scrape_date = _scrape_date_from_key(key)
    if resolved_scrape_date is None:
        raise ValueError(f"Could not parse scrape_date from S3 key: {key}")
    return list(reader), resolved_scrape_date, key


def save_silver_s3_csv(
    rows: list[dict[str, Any]],
    source: str,
    team: str,
    artifact_name: str,
    season: str,
    bucket: str,
    silver_prefix: str = "cleaned",
    scrape_date: Optional[str] = None,
) -> Optional[str]:
    if not rows:
        return None

    key = "/".join(
        [
            silver_prefix,
            source,
            team,
            artifact_name,
            season,
            f"scrape_date={resolve_scrape_date(scrape_date)}.csv",
        ]
    )

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    LOGGER.info("Wrote silver CSV data to s3://%s/%s", bucket, key)
    return key


def _latest_s3_key_for_prefix(
    bucket: str,
    prefix: str,
    suffix: str,
) -> Optional[str]:
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    candidate_keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(suffix):
                continue
            if _scrape_date_from_key(key) is None:
                continue
            candidate_keys.append(key)

    if not candidate_keys:
        return None
    return sorted(candidate_keys, key=_scrape_date_from_key)[-1]


def _scrape_date_from_key(key: str) -> Optional[str]:
    match = SCRAPE_DATE_KEY_PATTERN.search(key)
    if match is None:
        return None
    return match.group(1)


class S3Loader:
    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.s3_client = boto3.client("s3")

    def upload_files(
        self,
        files: Iterable[Path],
        bucket: Optional[str] = None,
        dry_run: bool = False,
    ) -> list[str]:
        target_bucket = bucket or self.config.S3_BUCKET
        uploaded_keys: list[str] = []

        for file_path in files:
            key = self.build_s3_key(file_path)
            if dry_run:
                LOGGER.info("Dry run: would upload %s to s3://%s/%s", file_path, target_bucket, key)
            else:
                self.s3_client.upload_file(str(file_path), target_bucket, key)
                LOGGER.info("Uploaded %s to s3://%s/%s", file_path, target_bucket, key)
            uploaded_keys.append(key)

        return uploaded_keys

    def build_s3_key(self, file_path: Path) -> str:
        bronze_root = Path(self.config.LOCAL_RAW_ROOT)
        silver_root = Path(self.config.LOCAL_CLEANED_ROOT)

        if bronze_root in file_path.parents:
            relative_path = file_path.relative_to(bronze_root)
            relative_parts = relative_path.parts
            if not relative_parts:
                raise ValueError(f"Could not build S3 key for path: {file_path}")

            if relative_parts[0] != "transfermarkt":
                raise ValueError(f"Unsupported local source root for S3 upload: {file_path}")

            if len(relative_parts) < 3:
                raise ValueError(f"Unsupported local artifact type for S3 upload: {file_path}")

            artifact_name = relative_parts[2]
            if artifact_name in {
                "team_roster",
                "player_detailed_stats_individual",
                "player_detailed_stats_combined",
            }:
                return str(Path(self.config.S3_RAW_PREFIX) / relative_path).replace("\\", "/")

            raise ValueError(f"Unsupported local artifact type for S3 upload: {file_path}")

        if silver_root in file_path.parents:
            relative_path = file_path.relative_to(silver_root)
            relative_parts = relative_path.parts
            if not relative_parts:
                raise ValueError(f"Could not build S3 key for path: {file_path}")
            if relative_parts[0] != "transfermarkt":
                raise ValueError(f"Unsupported local source root for S3 upload: {file_path}")
            return str(Path(self.config.S3_CLEANED_PREFIX) / relative_path).replace("\\", "/")

        raise ValueError(f"Unsupported local base path for S3 upload: {file_path}")

    def collect_local_files(
        self,
        season: Optional[str] = None,
        team: Optional[str] = None,
        include_cleaned: bool = True,
    ) -> list[Path]:
        bronze_root = Path(self.config.LOCAL_RAW_ROOT) / "transfermarkt"
        patterns = [
            "*/team_roster/**/*.json",
            "*/player_detailed_stats_individual/**/*.json",
            "*/player_detailed_stats_combined/**/*.csv",
        ]

        files: list[Path] = []
        for pattern in patterns:
            files.extend(bronze_root.glob(pattern))

        if include_cleaned:
            silver_root = Path(self.config.LOCAL_CLEANED_ROOT)
            files.extend(silver_root.glob("transfermarkt/*/**/*.csv"))

        if season:
            files = [path for path in files if season in path.parts]

        if team:
            files = [path for path in files if team in path.parts]

        return sorted(path for path in files if path.is_file())


def main() -> None:
    raise SystemExit(
        "Run the S3 loader via 'python scripts/run_s3_load.py' "
        "or 'python -m scripts.run_s3_load'. Direct execution of "
        "'src/loader/s3_loader.py' is unsupported."
    )


if __name__ == "__main__":
    main()
