from datetime import datetime

from pyspark.sql import functions as F

from pipeline_utils import build_spark, get_paths, normalise_columns, trim_string_columns


TABLE_CONFIG = {
    "patients": {
        "file_name": "patients.csv",
        "required_columns": ["patient_id", "name", "dob"],
    },
    "providers": {
        "file_name": "providers.csv",
        "required_columns": ["provider_id", "name", "specialty"],
    },
    "encounters": {
        "file_name": "encounters.csv",
        "required_columns": ["encounter_id", "patient_id", "date", "treatment"],
    },
    "cancer_catalog": {
        "file_name": "cancer_catalog.csv",
        "required_columns": ["patient_id", "cancer_type", "icd_code", "stage"],
    },
    "billing": {
        "file_name": "billing.csv",
        "required_columns": ["billing_id", "encounter_id", "patient_id", "amount"],
    },
}


def build_reject_condition(required_columns: list[str]):
    condition = F.lit(False)
    for column_name in required_columns:
        condition = condition | F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == "")
    return condition


def main():
    spark = build_spark("Cancer Raw to Bronze")
    paths = get_paths()
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    audit_rows = []

    for table_name, config in TABLE_CONFIG.items():
        raw_path = f"{paths['raw']}/{config['file_name']}"
        bronze_path = f"{paths['bronze']}/{table_name}"
        reject_path = f"{paths['ops']}/rejects/raw_to_bronze/{table_name}"

        print(f"\nIngesting {table_name} from {raw_path}")

        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(raw_path)
        )
        df = trim_string_columns(normalise_columns(df))
        source_count = df.count()

        reject_condition = build_reject_condition(config["required_columns"])
        rejected_df = (
            df.filter(reject_condition)
            .withColumn("reject_reason", F.lit("Missing required field"))
            .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
            .withColumn("rejected_at", F.current_timestamp())
        )
        valid_df = (
            df.filter(~reject_condition)
            .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file_name", F.lit(config["file_name"]))
        )

        reject_count = rejected_df.count()
        valid_count = valid_df.count()

        if reject_count:
            rejected_df.write.mode("overwrite").parquet(reject_path)

        valid_df.write.mode("overwrite").parquet(bronze_path)

        audit_rows.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "pipeline_step": "raw_to_bronze",
                "table_name": table_name,
                "source_count": source_count,
                "valid_count": valid_count,
                "reject_count": reject_count,
                "status": "SUCCESS",
            }
        )

    audit_df = (
        spark.createDataFrame(audit_rows)
        .withColumn("recorded_at", F.current_timestamp())
    )
    audit_df.write.mode("append").parquet(f"{paths['ops']}/audit/raw_to_bronze")

    print("\nRaw to bronze ingestion completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
