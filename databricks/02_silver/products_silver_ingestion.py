# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC * **Notebook**: customers_silver_ingestion
# MAGIC * **Layer**: silver
# MAGIC * **Table**: `retail_silver.customers`
# MAGIC * **Author**: Siddesh HK
# MAGIC

# COMMAND ----------



# COMMAND ----------

import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, max
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("silver products")
logger.info("Silver Products Notebook Started")

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
    logger.info("Reading Bronze Products Table")

    df_bronze = spark.table(source_table)

    bronze_count = df_bronze.count()
    logger.info(f"Bronze Product Count: {bronze_count}")

    if bronze_count == 0:
        raise Exception("No records found in Bronze products")

    latest_snapshot = df_bronze.agg(max("snapshot_date")).collect()[0][0]
    logger.info(f"Latest Snapshot: {latest_snapshot}")

    df_source = df_bronze.filter(col("snapshot_date") == latest_snapshot)

    logger.info(f"Snapshot Filtered Count: {df_source.count()}")

except Exception as e:
    logger.error("Error in Bronze Products Read")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Standardizing Product Columns")

    df_source = df_source \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("sku", trim(col("sku"))) \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("category", trim(col("category"))) \
        .withColumn("brand", trim(col("brand"))) \
        .withColumn("unit_price", trim(col("unit_price"))) \
        .withColumn("is_discontinued", upper(trim(col("is_discontinued"))))

    # Cast price safely
    df_source = df_source.withColumn(
        "unit_price",
        col("unit_price").cast("decimal(10,2)")
    )

    logger.info("Applying Mandatory Validation")

    df_valid = df_source.filter(
        (col("product_id").isNotNull()) & (col("product_id") != "") &
        (col("sku").isNotNull()) & (col("sku") != "") &
        (col("snapshot_date").isNotNull())
    )

    logger.info(f"Valid Product Count: {df_valid.count()}")

except Exception as e:
    logger.error("Error in Standardization / Validation")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("De-duplicating and Generating Hash for SCD2")
    window_spec = Window.partitionBy("product_id") \
                        .orderBy(col("ingestion_date").desc(),
                                 col("load_timestamp").desc())

    df_deduplicated = df_valid \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    df_final = df_deduplicated.withColumn(
        "hash_value",
        md5(concat_ws("||",
            col("sku"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("unit_price").cast("string"),
            col("is_discontinued")
        ))
    )

    df_final.createOrReplaceTempView("vw_product_source")

    logger.info("Temporary View vw_product_source Created")

except Exception as e:
    logger.error("Error in Hash Creation")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting Product SCD2 Update")

    spark.sql(f"""
        MERGE INTO {target_table} target
        USING vw_product_source source
        ON target.product_id = source.product_id
           AND target.is_current = 'Y'
        WHEN MATCHED
             AND target.hash_value <> source.hash_value
        THEN UPDATE SET
             effective_end_date = CURRENT_DATE(),
             is_current = 'N',
             updated_timestamp = CURRENT_TIMESTAMP()
    """)

    logger.info("Product SCD2 Update Completed")

except Exception as e:
    logger.error("Error in Product SCD2 Update")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting Product SCD2 Insert")

    spark.sql(f"""
        INSERT INTO {target_table} (
            product_id,
            sku,
            product_name,
            category,
            brand,
            unit_price,
            is_discontinued,
            hash_value,
            effective_start_date,
            effective_end_date,
            is_current,
            created_timestamp,
            updated_timestamp
        )
        SELECT
            source.product_id,
            source.sku,
            source.product_name,
            source.category,
            source.brand,
            source.unit_price,
            source.is_discontinued,
            source.hash_value,
            source.snapshot_date,
            DATE('9999-12-31'),
            'Y',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        FROM vw_product_source source
        LEFT JOIN {target_table} target
        ON source.product_id = target.product_id
        AND target.is_current = 'Y'
        WHERE target.product_id IS NULL
           OR target.hash_value <> source.hash_value
    """)

    logger.info("Product Silver Load Completed Successfully")

except Exception as e:
    logger.error("Error in Product SCD2 Insert")
    logger.error(str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.dim_products