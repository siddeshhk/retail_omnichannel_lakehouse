# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC * **Notebook**: customers_silver_ingestion
# MAGIC * **Layer**: silver
# MAGIC * **Table**: `retail_silver.customers`
# MAGIC * **Author**: Siddesh HK
# MAGIC

# COMMAND ----------

import logging
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, max
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("silver customers")
logger.info("Silver Customer Notebook Started")

# COMMAND ----------

try:
    # Create widgets
    dbutils.widgets.text("source_table", "")
    dbutils.widgets.text("target_table", "")

    # Get widget values
    source_table = dbutils.widgets.get("source_table")
    target_table = dbutils.widgets.get("target_table")

    if source_table == "" or target_table == "":
        raise Exception("Source or Target table parameter not passed")

except Exception as e:
    logger.error("Error in Bronze Read")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Reading Bronze Table")

    df_bronze = spark.table(source_table) \
        .filter(col("status") == "ACTIVE")

    bronze_count = df_bronze.count()
    logger.info(f"Active Bronze Record Count: {bronze_count}")

    if bronze_count == 0:
        raise Exception("No ACTIVE records found in Bronze table")

    latest_snapshot = df_bronze.agg(max("snapshot_date")).collect()[0][0]
    logger.info(f"Latest Snapshot Identified: {latest_snapshot}")

    df_source = df_bronze \
        .filter(col("snapshot_date") == latest_snapshot)

    logger.info(f"Snapshot Filtered Count: {df_source.count()}")

except Exception as e:
    logger.error("Error in Bronze Read Cell")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting Standardization")

    df_source = df_source \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("phone", trim(col("phone"))) \
        .withColumn("region", trim(col("region"))) \
        .withColumn("city", trim(col("city")))

    logger.info("Applying Mandatory Field Validation")

    df_valid_source = df_source.filter(
        (col("customer_id").isNotNull()) & (trim(col("customer_id")) != "") &
        (col("email").isNotNull()) & (trim(col("email")) != "") &
        (col("snapshot_date").isNotNull())
    )

    logger.info(f"After Mandatory Filter Count: {df_valid_source.count()}")

    logger.info("Validating Email Format")

    df_records_with_valid_email = df_valid_source.filter(
        col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")
    )

    logger.info(f"Valid Email Count: {df_records_with_valid_email.count()}")

    logger.info("Validating Phone Format")

    df_records_with_valid_phone = df_records_with_valid_email.filter(
        col("phone").rlike("^\\+?[0-9]+$")
    )

    logger.info(f"Valid Phone Count: {df_records_with_valid_phone.count()}")

except Exception as e:
    logger.error("Error in Standardization / Validation Cell")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting Deduplication")

    window_spec = Window.partitionBy("customer_id") \
                        .orderBy(col("ingestion_date").desc())

    df_deduplicated = df_records_with_valid_phone \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    logger.info(f"Deduplicated Count: {df_deduplicated.count()}")

    logger.info("Generating Hash Column")

    df_final_source = df_deduplicated.withColumn(
        "hash_value",
        md5(concat_ws("||",
            col("customer_name"),
            col("email"),
            col("phone"),
            col("region"),
            col("city"),
            col("status")
        ))
    )

    df_final_source.createOrReplaceTempView("vw_customer_source")

    logger.info("Temporary View vw_customer_source Created")

except Exception as e:
    logger.error("Error in Deduplication / Hash Cell")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting SCD2 Update (Expire Old Records)")

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING vw_customer_source AS source
        ON target.customer_id = source.customer_id
           AND target.is_current = 'Y'
        WHEN MATCHED
             AND target.hash_value <> source.hash_value
        THEN UPDATE SET
             target.effective_end_date = CURRENT_DATE(),
             target.is_current = 'N',
             target.status = 'INACTIVE',
             target.updated_timestamp = CURRENT_TIMESTAMP()
    """)

    logger.info("SCD2 Update Completed Successfully")

except Exception as e:
    logger.error("Error in SCD2 Update Cell")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting SCD2 Insert")

    spark.sql(f"""
        INSERT INTO {target_table} (
            customer_id,
            customer_name,
            email,
            phone,
            region,
            city,
            signup_date,
            status,
            hash_value,
            effective_start_date,
            effective_end_date,
            is_current,
            created_timestamp,
            updated_timestamp
        )
        SELECT
            source.customer_id,
            source.customer_name,
            source.email,
            source.phone,
            source.region,
            source.city,
            source.signup_date,
            source.status,
            source.hash_value,
            source.snapshot_date,
            DATE('9999-12-31'),
            'Y',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        FROM vw_customer_source source
        LEFT JOIN {target_table} target
        ON source.customer_id = target.customer_id
        AND target.is_current = 'Y'
        WHERE target.customer_id IS NULL
           OR target.hash_value <> source.hash_value
    """)

    logger.info("SCD2 Insert Completed Successfully")
    logger.info("Silver Customer Load Completed Successfully")

except Exception as e:
    logger.error("Error in SCD2 Insert Cell")
    logger.error(str(e))
    raise