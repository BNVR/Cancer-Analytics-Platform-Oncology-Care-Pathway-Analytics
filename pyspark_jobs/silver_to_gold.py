from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline_utils import build_spark, get_paths, spark_path_exists


def with_record_hash(df, tracked_columns):
    hash_inputs = [F.coalesce(F.col(column_name).cast("string"), F.lit("")) for column_name in tracked_columns]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *hash_inputs), 256))


def read_existing_dimension(spark, path, schema):
    if spark_path_exists(spark, path):
        return spark.read.parquet(path)
    return spark.createDataFrame([], schema)


def apply_scd_type_2(spark, incoming_df, existing_path, business_keys, surrogate_key, tracked_columns):
    staged_df = with_record_hash(incoming_df, tracked_columns)
    existing_df = read_existing_dimension(spark, existing_path, staged_df.schema.add(surrogate_key, "long"))

    if existing_df.rdd.isEmpty():
        seed_window = Window.orderBy(*business_keys)
        return (
            staged_df.withColumn(surrogate_key, F.row_number().over(seed_window).cast("long"))
            .withColumn("valid_from", F.current_timestamp())
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

    current_df = existing_df.filter(F.col("is_current") == True)
    historical_df = existing_df.filter(F.col("is_current") == False)

    join_condition = [current_df[key] == staged_df[key] for key in business_keys]
    joined_df = staged_df.alias("incoming").join(current_df.alias("current"), join_condition, "left")

    unchanged_df = (
        joined_df.filter(F.col("current.record_hash") == F.col("incoming.record_hash"))
        .select("current.*")
    )

    carried_forward_df = (
        current_df.alias("current")
        .join(staged_df.alias("incoming"), business_keys, "left_anti")
        .select("current.*")
    )

    expired_df = (
        joined_df.filter(
            F.col("current.record_hash").isNotNull()
            & (F.col("current.record_hash") != F.col("incoming.record_hash"))
        )
        .select("current.*")
        .withColumn("valid_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False))
    )

    new_versions_df = (
        joined_df.filter(
            F.col("current.record_hash").isNull()
            | (F.col("current.record_hash") != F.col("incoming.record_hash"))
        )
        .select("incoming.*")
    )

    current_max_key = existing_df.agg(F.coalesce(F.max(surrogate_key), F.lit(0)).alias("max_key")).collect()[0]["max_key"]
    new_key_window = Window.orderBy(*business_keys)
    new_versions_df = (
        new_versions_df.withColumn(
            surrogate_key,
            (F.row_number().over(new_key_window) + F.lit(int(current_max_key))).cast("long"),
        )
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    return historical_df.unionByName(unchanged_df).unionByName(carried_forward_df).unionByName(expired_df).unionByName(new_versions_df)


def build_dim_date(encounters_df):
    return (
        encounters_df.select(F.col("encounter_date"))
        .dropna()
        .dropDuplicates()
        .withColumn("date_sk", F.date_format("encounter_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("encounter_date"))
        .withColumn("month", F.month("encounter_date"))
        .withColumn("day", F.dayofmonth("encounter_date"))
        .withColumn("quarter", F.quarter("encounter_date"))
    )


def main():
    spark = build_spark("Cancer Analytics Gold Layer")
    paths = get_paths()
    pipeline_run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    silver_base = paths["silver"]
    gold_base = paths["gold"]
    ops_base = paths["ops"]

    patients = spark.read.parquet(f"{silver_base}/patients")
    providers = spark.read.parquet(f"{silver_base}/providers")
    encounters = spark.read.parquet(f"{silver_base}/encounters")
    billing = spark.read.parquet(f"{silver_base}/billing")
    cancer_catalog = spark.read.parquet(f"{silver_base}/cancer_catalog")

    dim_patients_incoming = patients.select(
        "patient_id",
        F.col("name").alias("patient_name"),
        "dob",
        "gender",
        "stage",
        "bmi",
        "smoking_status",
        "family_history_cancer",
        "radiation_exposure",
        "previous_cancers",
        "age",
    ).dropDuplicates(["patient_id"])

    dim_cancer_incoming = cancer_catalog.select(
        "patient_id",
        F.col("patient_name"),
        "cancer_type",
        "icd_code",
        "stage",
        "description",
        "tumor_markers",
    ).dropDuplicates(["patient_id", "cancer_type"])

    dim_patients = apply_scd_type_2(
        spark=spark,
        incoming_df=dim_patients_incoming,
        existing_path=f"{gold_base}/dim_patients",
        business_keys=["patient_id"],
        surrogate_key="patient_sk",
        tracked_columns=[
            "patient_name",
            "dob",
            "gender",
            "stage",
            "bmi",
            "smoking_status",
            "family_history_cancer",
            "radiation_exposure",
            "previous_cancers",
            "age",
        ],
    )

    dim_cancer = apply_scd_type_2(
        spark=spark,
        incoming_df=dim_cancer_incoming,
        existing_path=f"{gold_base}/dim_cancer_catalog",
        business_keys=["patient_id", "cancer_type"],
        surrogate_key="cancer_sk",
        tracked_columns=[
            "patient_name",
            "icd_code",
            "stage",
            "description",
            "tumor_markers",
        ],
    )

    provider_window = Window.orderBy("provider_id")
    dim_providers = (
        providers.select(
            "provider_id",
            F.col("name").alias("provider_name"),
            "specialty",
            "location",
            F.col("years_of_experience").alias("experience_years"),
        )
        .dropDuplicates(["provider_id"])
        .withColumn("provider_sk", F.row_number().over(provider_window).cast("long"))
        .withColumn("loaded_at", F.current_timestamp())
    )

    dim_date = build_dim_date(encounters).withColumn("loaded_at", F.current_timestamp())

    current_patients = dim_patients.filter(F.col("is_current") == True).select("patient_id", "patient_sk")
    current_cancer = dim_cancer.filter(F.col("is_current") == True).select("patient_id", "cancer_type", "cancer_sk")
    cancer_by_patient = (
        current_cancer.withColumn(
            "row_num",
            F.row_number().over(Window.partitionBy("patient_id").orderBy("cancer_type")),
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num", "cancer_type")
    )

    fact_encounters = (
        encounters.alias("enc")
        .join(current_patients.alias("pat"), "patient_id", "left")
        .join(cancer_by_patient.alias("can"), "patient_id", "left")
        .join(billing.alias("bill"), "encounter_id", "left")
        .join(dim_date.select("encounter_date", "date_sk").alias("dt"), "encounter_date", "left")
        .select(
            F.col("enc.encounter_id"),
            F.col("pat.patient_sk"),
            F.col("can.cancer_sk"),
            F.col("dt.date_sk"),
            F.col("enc.patient_id"),
            F.col("enc.encounter_date"),
            F.col("enc.treatment"),
            F.col("enc.drug_regimen"),
            F.col("enc.number_of_cycles"),
            F.col("enc.treatment_response_score"),
            F.col("enc.outcome"),
            F.col("bill.billing_id"),
            F.col("bill.amount").alias("billing_amount"),
            F.col("bill.insurance"),
            F.col("bill.payment_status"),
        )
        .dropDuplicates(["encounter_id"])
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("loaded_at", F.current_timestamp())
    )

    dim_patients.write.mode("overwrite").parquet(f"{gold_base}/dim_patients")
    dim_cancer.write.mode("overwrite").parquet(f"{gold_base}/dim_cancer_catalog")
    dim_providers.write.mode("overwrite").parquet(f"{gold_base}/dim_providers")
    dim_date.write.mode("overwrite").parquet(f"{gold_base}/dim_date")
    fact_encounters.write.mode("overwrite").parquet(f"{gold_base}/fact_encounters")

    audit_rows = [
        {"table_name": "dim_patients", "row_count": dim_patients.count()},
        {"table_name": "dim_cancer_catalog", "row_count": dim_cancer.count()},
        {"table_name": "dim_providers", "row_count": dim_providers.count()},
        {"table_name": "dim_date", "row_count": dim_date.count()},
        {"table_name": "fact_encounters", "row_count": fact_encounters.count()},
    ]

    audit_df = (
        spark.createDataFrame(audit_rows)
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("pipeline_step", F.lit("silver_to_gold"))
        .withColumn("status", F.lit("SUCCESS"))
        .withColumn("recorded_at", F.current_timestamp())
    )
    audit_df.write.mode("append").parquet(f"{ops_base}/audit/silver_to_gold")

    print("Gold layer created successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
