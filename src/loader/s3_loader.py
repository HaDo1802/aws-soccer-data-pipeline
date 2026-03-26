from pathlib import Path
from typing import Iterable, Optional

import boto3

from utils.config import Config
from utils.logger import get_logger


LOGGER = get_logger(__name__)


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
        bronze_root = Path(self.config.LOCAL_BRONZE_ROOT)
        silver_root = Path(self.config.LOCAL_SILVER_ROOT)

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
                return str(Path(self.config.S3_BRONZE_PREFIX) / relative_path).replace("\\", "/")

            raise ValueError(f"Unsupported local artifact type for S3 upload: {file_path}")

        if silver_root in file_path.parents:
            relative_path = file_path.relative_to(silver_root)
            relative_parts = relative_path.parts
            if not relative_parts:
                raise ValueError(f"Could not build S3 key for path: {file_path}")
            if relative_parts[0] != "transfermarkt":
                raise ValueError(f"Unsupported local source root for S3 upload: {file_path}")
            return str(Path(self.config.S3_SILVER_PREFIX) / relative_path).replace("\\", "/")

        raise ValueError(f"Unsupported local base path for S3 upload: {file_path}")

    def collect_local_files(
        self,
        season: Optional[str] = None,
        team: Optional[str] = None,
        include_cleaned: bool = True,
    ) -> list[Path]:
        bronze_root = Path(self.config.LOCAL_BRONZE_ROOT) / "transfermarkt"
        patterns = [
            "*/team_roster/**/*.json",
            "*/player_detailed_stats_individual/**/*.json",
            "*/player_detailed_stats_combined/**/*.csv",
        ]

        files: list[Path] = []
        for pattern in patterns:
            files.extend(bronze_root.glob(pattern))

        if include_cleaned:
            silver_root = Path(self.config.LOCAL_SILVER_ROOT)
            files.extend(silver_root.glob("transfermarkt/*/**/*.parquet"))

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
