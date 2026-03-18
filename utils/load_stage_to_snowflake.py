from __future__ import annotations

from datetime import datetime, UTC
import os
from pathlib import Path

import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

TABLE_FILE_MAP = {
    "PATIENTS": "patients.csv",
    "PROVIDERS": "providers.csv",
    "ENCOUNTERS": "encounters.csv",
    "CANCER_CATALOG": "cancer_catalog.csv",
    "BILLING": "billing.csv",
}


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CANCER_ANALYTICS_DB"),
        schema="RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def stage_file(cur, csv_path: Path) -> None:
    stage_path = f"@RAW.RAW_CSV_STAGE/{csv_path.name}"
    cur.execute(f"REMOVE {stage_path}")
    cur.execute(f"PUT 'file://{csv_path.as_posix()}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def load_table(cur, table_name: str, file_name: str, pipeline_run_id: str) -> None:
    cur.execute(f"TRUNCATE TABLE RAW.{table_name}")

    if table_name == "PATIENTS":
        select_sql = f"""
        SELECT
            $1::STRING,
            $2::STRING,
            TRY_TO_DATE($3),
            $4::STRING,
            $5::STRING,
            TRY_TO_DECIMAL($6, 10, 2),
            $7::STRING,
            $8::STRING,
            $9::STRING,
            $10::STRING,
            TRY_TO_NUMBER($11),
            '{pipeline_run_id}',
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @RAW.RAW_CSV_STAGE/{file_name}
        """
    elif table_name == "PROVIDERS":
        select_sql = f"""
        SELECT
            $1::STRING,
            $2::STRING,
            $3::STRING,
            $4::STRING,
            TRY_TO_NUMBER($5),
            '{pipeline_run_id}',
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @RAW.RAW_CSV_STAGE/{file_name}
        """
    elif table_name == "ENCOUNTERS":
        select_sql = f"""
        SELECT
            $1::STRING,
            $2::STRING,
            TRY_TO_DATE($3),
            $4::STRING,
            $5::STRING,
            TRY_TO_NUMBER($6),
            TRY_TO_DECIMAL($7, 10, 2),
            $8::STRING,
            '{pipeline_run_id}',
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @RAW.RAW_CSV_STAGE/{file_name}
        """
    elif table_name == "CANCER_CATALOG":
        select_sql = f"""
        SELECT
            $1::STRING,
            $2::STRING,
            $3::STRING,
            $4::STRING,
            $5::STRING,
            $6::STRING,
            $7::STRING,
            '{pipeline_run_id}',
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @RAW.RAW_CSV_STAGE/{file_name}
        """
    else:
        select_sql = f"""
        SELECT
            $1::STRING,
            $2::STRING,
            $3::STRING,
            $4::STRING,
            TRY_TO_DECIMAL($5, 18, 2),
            $6::STRING,
            $7::STRING,
            '{pipeline_run_id}',
            CURRENT_TIMESTAMP(),
            METADATA$FILENAME
        FROM @RAW.RAW_CSV_STAGE/{file_name}
        """

    cur.execute(f"INSERT INTO RAW.{table_name} {select_sql}")


def main() -> None:
    pipeline_run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("USE DATABASE CANCER_ANALYTICS_DB")
            cur.execute("USE SCHEMA RAW")

            for table_name, file_name in TABLE_FILE_MAP.items():
                csv_path = RAW_DATA_DIR / file_name
                print(f"Uploading {csv_path.name} to RAW.RAW_CSV_STAGE")
                stage_file(cur, csv_path)
                print(f"Loading staged file into RAW.{table_name}")
                load_table(cur, table_name, file_name, pipeline_run_id)


if __name__ == "__main__":
    main()
