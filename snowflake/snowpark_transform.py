from __future__ import annotations

import os
from uuid import uuid4

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    avg,
    call_builtin,
    col,
    concat_ws,
    count_distinct,
    coalesce,
    current_timestamp,
    lit,
    max as sf_max,
    month,
    quarter,
    row_number,
    sha2,
    sum as sf_sum,
    to_char,
    year,
    dayofmonth,
)
from snowflake.snowpark.window import Window


def build_session() -> Session:
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "CANCER_ANALYTICS_DB"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }
    return Session.builder.configs(connection_parameters).create()


def latest_per_key(df, partition_cols):
    window_spec = Window.partition_by(*partition_cols).order_by(col("INGESTED_AT").desc_nulls_last())
    return df.with_column("ROW_NUM", row_number().over(window_spec)).filter(col("ROW_NUM") == 1).drop("ROW_NUM")


def add_patient_hash(df):
    return df.with_column(
        "RECORD_HASH",
        sha2(
            concat_ws(
                lit("||"),
                coalesce(col("PATIENT_NAME"), lit("")),
                coalesce(col("DOB").cast("string"), lit("")),
                coalesce(col("GENDER"), lit("")),
                coalesce(col("STAGE"), lit("")),
                coalesce(col("BMI").cast("string"), lit("")),
                coalesce(col("SMOKING_STATUS"), lit("")),
                coalesce(col("FAMILY_HISTORY_CANCER"), lit("")),
                coalesce(col("RADIATION_EXPOSURE"), lit("")),
                coalesce(col("PREVIOUS_CANCERS"), lit("")),
                coalesce(col("AGE").cast("string"), lit("")),
            ),
            256,
        ),
    )


def add_cancer_hash(df):
    return df.with_column(
        "RECORD_HASH",
        sha2(
            concat_ws(
                lit("||"),
                coalesce(col("PATIENT_NAME"), lit("")),
                coalesce(col("CANCER_TYPE"), lit("")),
                coalesce(col("ICD_CODE"), lit("")),
                coalesce(col("STAGE"), lit("")),
                coalesce(col("DESCRIPTION"), lit("")),
                coalesce(col("TUMOR_MARKERS"), lit("")),
            ),
            256,
        ),
    )


def merge_scd2(session: Session, source_df, target_table: str, business_keys: list[str], compare_hash: str = "RECORD_HASH"):
    temp_table = f'TEMP_{target_table.replace(".", "_")}_{uuid4().hex[:8]}'
    source_df.write.mode("overwrite").save_as_table(temp_table, table_type="temporary")

    join_condition = " AND ".join([f'TARGET."{key}" = SRC."{key}"' for key in business_keys])
    current_join_condition = " AND ".join([f'CURR."{key}" = SRC."{key}"' for key in business_keys])
    insert_columns = ", ".join([f'"{column}"' for column in source_df.columns])
    insert_values = ", ".join([f'SRC."{column}"' for column in source_df.columns])
    null_check_column = business_keys[0]

    session.sql(
        f"""
        UPDATE {target_table} TARGET
        SET VALID_TO = CURRENT_TIMESTAMP(),
            IS_CURRENT = FALSE
        FROM {temp_table} SRC
        WHERE {join_condition}
          AND TARGET.IS_CURRENT = TRUE
          AND TARGET."{compare_hash}" <> SRC."{compare_hash}"
        """
    ).collect()

    session.sql(
        f"""
        INSERT INTO {target_table} ({insert_columns})
        SELECT {insert_values}
        FROM {temp_table} SRC
        LEFT JOIN {target_table} CURR
            ON {current_join_condition}
           AND CURR.IS_CURRENT = TRUE
        WHERE CURR."{null_check_column}" IS NULL
           OR CURR."{compare_hash}" <> SRC."{compare_hash}"
        """
    ).collect()

    session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()


def main():
    session = build_session()
    session.use_database(os.environ.get("SNOWFLAKE_DATABASE", "CANCER_ANALYTICS_DB"))
    session.use_schema("RAW")

    patients_raw = latest_per_key(session.table("RAW.PATIENTS"), ["PATIENT_ID"]).select(
        col("PATIENT_ID"),
        col("NAME").alias("PATIENT_NAME"),
        col("DOB"),
        col("GENDER"),
        col("STAGE"),
        col("BMI"),
        col("SMOKING_STATUS"),
        col("FAMILY_HISTORY_CANCER"),
        col("RADIATION_EXPOSURE"),
        col("PREVIOUS_CANCERS"),
        col("AGE"),
    )
    patients_stg = (
        add_patient_hash(patients_raw)
        .with_column("VALID_FROM", current_timestamp())
        .with_column("VALID_TO", lit(None).cast("timestamp"))
        .with_column("IS_CURRENT", lit(True))
    )
    merge_scd2(session, patients_stg, "DW.DIM_PATIENTS", ["PATIENT_ID"])

    cancer_raw = latest_per_key(session.table("RAW.CANCER_CATALOG"), ["PATIENT_ID", "CANCER_TYPE"]).select(
        "PATIENT_ID",
        "PATIENT_NAME",
        "CANCER_TYPE",
        "ICD_CODE",
        "STAGE",
        "DESCRIPTION",
        "TUMOR_MARKERS",
    )
    cancer_stg = (
        add_cancer_hash(cancer_raw)
        .with_column("VALID_FROM", current_timestamp())
        .with_column("VALID_TO", lit(None).cast("timestamp"))
        .with_column("IS_CURRENT", lit(True))
    )
    merge_scd2(session, cancer_stg, "DW.DIM_CANCER_CATALOG", ["PATIENT_ID", "CANCER_TYPE"])

    providers_stg = latest_per_key(session.table("RAW.PROVIDERS"), ["PROVIDER_ID"]).select(
        col("PROVIDER_ID"),
        col("NAME").alias("PROVIDER_NAME"),
        col("SPECIALTY"),
        col("LOCATION"),
        col("YEARS_OF_EXPERIENCE").alias("EXPERIENCE_YEARS"),
        current_timestamp().alias("LOADED_AT"),
    )
    session.sql("TRUNCATE TABLE DW.DIM_PROVIDERS").collect()
    providers_stg.write.mode("append").save_as_table("DW.DIM_PROVIDERS", column_order="name")

    encounters = session.table("RAW.ENCOUNTERS")
    dim_date = (
        encounters.filter(col("DATE").is_not_null())
        .select(col("DATE").alias("ENCOUNTER_DATE"))
        .distinct()
        .with_column("DATE_SK", call_builtin("TO_NUMBER", to_char(col("ENCOUNTER_DATE"), lit("YYYYMMDD"))))
        .with_column("YEAR", year(col("ENCOUNTER_DATE")))
        .with_column("MONTH", month(col("ENCOUNTER_DATE")))
        .with_column("DAY", dayofmonth(col("ENCOUNTER_DATE")))
        .with_column("QUARTER", quarter(col("ENCOUNTER_DATE")))
        .with_column("LOADED_AT", current_timestamp())
    )
    dim_date.write.mode("overwrite").save_as_table("DW.DIM_DATE")

    session.sql("TRUNCATE TABLE DW.FACT_ENCOUNTERS").collect()
    session.sql(
        """
        INSERT INTO DW.FACT_ENCOUNTERS (
            ENCOUNTER_ID,
            PATIENT_SK,
            CANCER_SK,
            PROVIDER_SK,
            PROVIDER_ID,
            DATE_SK,
            PATIENT_ID,
            ENCOUNTER_DATE,
            TREATMENT,
            DRUG_REGIMEN,
            NUMBER_OF_CYCLES,
            TREATMENT_RESPONSE_SCORE,
            OUTCOME,
            BILLING_ID,
            BILLING_AMOUNT,
            INSURANCE,
            PAYMENT_STATUS,
            PIPELINE_RUN_ID,
            LOADED_AT
        )
        SELECT
            E.ENCOUNTER_ID,
            P.PATIENT_SK,
            C.CANCER_SK,
            PR.PROVIDER_SK,
            PR.PROVIDER_ID,
            D.DATE_SK,
            E.PATIENT_ID,
            E.DATE AS ENCOUNTER_DATE,
            E.TREATMENT,
            E.DRUG_REGIMEN,
            E.NUMBER_OF_CYCLES,
            E.TREATMENT_RESPONSE_SCORE,
            E.OUTCOME,
            B.BILLING_ID,
            B.AMOUNT AS BILLING_AMOUNT,
            B.INSURANCE,
            B.PAYMENT_STATUS,
            E.PIPELINE_RUN_ID,
            CURRENT_TIMESTAMP()
        FROM RAW.ENCOUNTERS E
        LEFT JOIN DW.DIM_PATIENTS P
            ON E.PATIENT_ID = P.PATIENT_ID
           AND P.IS_CURRENT = TRUE
        LEFT JOIN DW.DIM_CANCER_CATALOG C
            ON E.PATIENT_ID = C.PATIENT_ID
           AND C.IS_CURRENT = TRUE
        LEFT JOIN (
            SELECT
                PROVIDER_SK,
                PROVIDER_ID,
                ROW_NUMBER() OVER (ORDER BY PROVIDER_ID) - 1 AS PROVIDER_INDEX
            FROM DW.DIM_PROVIDERS
        ) PR
            ON MOD(ABS(HASH(E.ENCOUNTER_ID)), (SELECT COUNT(*) FROM DW.DIM_PROVIDERS)) = PR.PROVIDER_INDEX
        LEFT JOIN DW.DIM_DATE D
            ON E.DATE = D.ENCOUNTER_DATE
        LEFT JOIN RAW.BILLING B
            ON E.ENCOUNTER_ID = B.ENCOUNTER_ID
        """
    ).collect()
    fact = session.table("DW.FACT_ENCOUNTERS")

    session.sql("TRUNCATE TABLE DW.BRIDGE_ENCOUNTER_PROVIDER").collect()
    session.sql(
        """
        INSERT INTO DW.BRIDGE_ENCOUNTER_PROVIDER (
            ENCOUNTER_ID,
            PROVIDER_SK,
            PROVIDER_ID,
            ASSIGNMENT_METHOD,
            LOADED_AT
        )
        SELECT
            ENCOUNTER_ID,
            PROVIDER_SK,
            PROVIDER_ID,
            'HASH_ENCOUNTER_PROVIDER_ASSIGNMENT',
            CURRENT_TIMESTAMP()
        FROM DW.FACT_ENCOUNTERS
        WHERE PROVIDER_SK IS NOT NULL
        """
    ).collect()

    session.sql(
        """
        CREATE OR REPLACE VIEW SEMANTIC.V_ENCOUNTER_OVERVIEW AS
        SELECT
            F.ENCOUNTER_ID,
            F.ENCOUNTER_DATE,
            F.TREATMENT,
            F.DRUG_REGIMEN,
            F.NUMBER_OF_CYCLES,
            F.TREATMENT_RESPONSE_SCORE,
            F.OUTCOME,
            F.BILLING_AMOUNT,
            F.INSURANCE,
            F.PAYMENT_STATUS,
            P.PATIENT_ID,
            P.PATIENT_NAME,
            P.GENDER,
            P.AGE,
            P.SMOKING_STATUS,
            C.CANCER_TYPE,
            C.STAGE
        FROM DW.FACT_ENCOUNTERS F
        LEFT JOIN DW.DIM_PATIENTS_CURRENT P
            ON F.PATIENT_SK = P.PATIENT_SK
        LEFT JOIN DW.DIM_CANCER_CATALOG_CURRENT C
            ON F.CANCER_SK = C.CANCER_SK
        LEFT JOIN DW.DIM_PROVIDERS PR
            ON F.PROVIDER_SK = PR.PROVIDER_SK
        """
    ).collect()

    session.sql(
        """
        CREATE OR REPLACE VIEW SEMANTIC.V_FINANCIAL_ANALYTICS AS
        SELECT
            ENCOUNTER_DATE,
            TREATMENT,
            INSURANCE,
            PAYMENT_STATUS,
            COUNT(DISTINCT ENCOUNTER_ID) AS ENCOUNTER_COUNT,
            SUM(BILLING_AMOUNT) AS TOTAL_BILLING_AMOUNT,
            AVG(BILLING_AMOUNT) AS AVG_BILLING_AMOUNT
        FROM DW.FACT_ENCOUNTERS
        GROUP BY 1,2,3,4
        """
    ).collect()

    session.sql(
        """
        CREATE OR REPLACE VIEW SEMANTIC.V_PROVIDER_PERFORMANCE AS
        SELECT
            F.PROVIDER_ID,
            PR.PROVIDER_NAME,
            PR.SPECIALTY,
            PR.LOCATION,
            COUNT(DISTINCT F.ENCOUNTER_ID) AS ENCOUNTER_COUNT,
            COUNT(DISTINCT F.PATIENT_ID) AS PATIENT_COUNT,
            AVG(F.TREATMENT_RESPONSE_SCORE) AS AVG_TREATMENT_RESPONSE_SCORE,
            SUM(F.BILLING_AMOUNT) AS TOTAL_BILLING_AMOUNT,
            AVG(F.BILLING_AMOUNT) AS AVG_BILLING_AMOUNT
        FROM DW.FACT_ENCOUNTERS F
        LEFT JOIN DW.DIM_PROVIDERS PR
            ON F.PROVIDER_SK = PR.PROVIDER_SK
        GROUP BY 1,2,3,4
        """
    ).collect()

    max_run_id = encounters.select(sf_max(col("PIPELINE_RUN_ID")).alias("PIPELINE_RUN_ID")).collect()[0]["PIPELINE_RUN_ID"]
    audit_df = session.create_dataframe(
        [
            [
                max_run_id,
                "snowpark_raw_to_dw",
                "fact_encounters",
                fact.count(),
                fact.count(),
                0,
                fact.count(),
                "SUCCESS",
            ]
        ],
        schema=[
            "PIPELINE_RUN_ID",
            "PIPELINE_STEP",
            "TABLE_NAME",
            "SOURCE_COUNT",
            "VALID_COUNT",
            "REJECT_COUNT",
            "ROW_COUNT",
            "STATUS",
        ],
    ).with_column("RECORDED_AT", current_timestamp())
    audit_df.write.mode("append").save_as_table("OPS.PIPELINE_AUDIT")

    session.close()


if __name__ == "__main__":
    main()
