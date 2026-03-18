from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from pipeline_utils import build_spark, get_paths, spark_path_exists, trim_string_columns


TABLES = ["billing", "patients", "providers", "encounters", "cancer_catalog"]


def clean_name_columns(df):
    cleaned_df = df
    for column_name in df.columns:
        if "name" in column_name:
            cleaned_df = cleaned_df.withColumn(
                column_name,
                F.regexp_replace(F.col(column_name), r"\s\d+$", ""),
            )
    return cleaned_df


def apply_table_specific_transforms(table_name, df):
    transformed_df = df

    if table_name == "patients":
        transformed_df = (
            transformed_df.withColumn("dob", F.to_date("dob"))
            .withColumn("age", F.col("age").cast(IntegerType()))
            .withColumn("bmi", F.col("bmi").cast(DoubleType()))
        )
    elif table_name == "providers":
        transformed_df = transformed_df.withColumn(
            "years_of_experience", F.col("years_of_experience").cast(IntegerType())
        )
    elif table_name == "encounters":
        transformed_df = (
            transformed_df.withColumn("encounter_date", F.to_date("date"))
            .drop("date")
            .withColumn("number_of_cycles", F.col("number_of_cycles").cast(IntegerType()))
            .withColumn(
                "treatment_response_score",
                F.col("treatment_response_score").cast(DoubleType()),
            )
        )
    elif table_name == "billing":
        transformed_df = transformed_df.withColumn("amount", F.col("amount").cast(DoubleType()))

    return transformed_df


def build_reject_condition(table_name):
    if table_name == "patients":
        return F.col("patient_id").isNull() | F.col("dob").isNull() | F.col("age").isNull()
    if table_name == "providers":
        return F.col("provider_id").isNull() | F.col("years_of_experience").isNull()
    if table_name == "encounters":
        return (
            F.col("encounter_id").isNull()
            | F.col("patient_id").isNull()
            | F.col("encounter_date").isNull()
        )
    if table_name == "billing":
        return F.col("billing_id").isNull() | F.col("encounter_id").isNull() | F.col("amount").isNull()
    if table_name == "cancer_catalog":
        return F.col("patient_id").isNull() | F.col("cancer_type").isNull() | F.col("icd_code").isNull()
    return F.lit(False)


def main():
    spark = build_spark("Cancer Bronze to Silver")
    paths = get_paths()
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    audit_rows = []

    for table_name in TABLES:
        bronze_path = f"{paths['bronze']}/{table_name}"
        silver_path = f"{paths['silver']}/{table_name}"
        reject_path = f"{paths['ops']}/rejects/bronze_to_silver/{table_name}"

        if not spark_path_exists(spark, bronze_path):
            raise FileNotFoundError(f"Bronze dataset not found for {table_name}: {bronze_path}")

        print(f"\nTransforming {table_name} from bronze to silver")

        df = spark.read.parquet(bronze_path)
        source_count = df.count()

        df = trim_string_columns(df.dropDuplicates())
        df = clean_name_columns(df)
        df = apply_table_specific_transforms(table_name, df)

        reject_condition = build_reject_condition(table_name)
        rejected_df = (
            df.filter(reject_condition)
            .withColumn("reject_reason", F.lit("Silver validation failed"))
            .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
            .withColumn("rejected_at", F.current_timestamp())
        )
        valid_df = (
            df.filter(~reject_condition)
            .withColumn("silver_processed_at", F.current_timestamp())
        )

        reject_count = rejected_df.count()
        valid_count = valid_df.count()

        if reject_count:
            rejected_df.write.mode("overwrite").parquet(reject_path)

        valid_df.write.mode("overwrite").parquet(silver_path)

        audit_rows.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "pipeline_step": "bronze_to_silver",
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
    audit_df.write.mode("append").parquet(f"{paths['ops']}/audit/bronze_to_silver")

    print("\nBronze to silver transformations completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
