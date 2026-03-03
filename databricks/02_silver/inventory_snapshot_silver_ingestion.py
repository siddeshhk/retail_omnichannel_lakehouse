# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC * **Notebook**: inventory_snapshot_silver_ingestion
# MAGIC * **Layer**: silver
# MAGIC * **Table**: `retail_silver.inventory_snapshot`
# MAGIC * **Author**: Siddesh HK
# MAGIC

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

logger = logging.getLogger("SILVER_CUSTOMER")
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
    logger.info("Reading Bronze Inventory Table")

    df_bronze = spark.table(source_table)

    bronze_count = df_bronze.count()
    logger.info(f"Bronze Inventory Count: {bronze_count}")

    if bronze_count == 0:
        raise Exception("No records found in Bronze inventory_snapshot")

    df_source = df_bronze

except Exception as e:
    logger.error("Error in Bronze Read")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Standardizing Inventory Columns")

    df_source = df_source \
        .withColumn("snapshot_date", to_date(col("snapshot_date"))) \
        .withColumn("store_id", trim(col("store_id"))) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("on_hand_qty", trim(col("on_hand_qty"))) \
        .withColumn("reorder_point", trim(col("reorder_point"))) \
        .withColumn("source_system", trim(col("source_system")))

    # Safe numeric casting
    df_source = df_source \
        .withColumn(
            "on_hand_qty",
            when(col("on_hand_qty").rlike("^[0-9]+$"),
                 col("on_hand_qty").cast("int")
            ).otherwise(None)
        ) \
        .withColumn(
            "reorder_point",
            when(col("reorder_point").rlike("^[0-9]+$"),
                 col("reorder_point").cast("int")
            ).otherwise(None)
        )

    logger.info("Applying Mandatory Validation")

    df_valid = df_source.filter(
        col("snapshot_date").isNotNull() &
        col("store_id").isNotNull() & (col("store_id") != "") &
        col("product_id").isNotNull() & (col("product_id") != "")
    )

    logger.info(f"Valid Inventory Count: {df_valid.count()}")

except Exception as e:
    logger.error("Error in Standardization / Validation")
    logger.error(str(e))
    raise

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

try:
    logger.info("Applying Deduplication")

    window_spec = Window.partitionBy(
        "snapshot_date", "store_id", "product_id"
    ).orderBy(
        col("ingestion_date").desc(),
        col("load_timestamp").desc()
    )

    df_deduplicated = df_valid \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
        
    df_deduplicated.createOrReplaceTempView("vw_inventory_source")
    logger.info(f"Deduplicated Count: {df_deduplicated.count()}")

except Exception as e:
    logger.error("Error in Deduplication")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Starting Inventory UPSERT")

    spark.sql("""
        MERGE INTO retail_silver.inventory_snapshot target
        USING vw_inventory_source source
        ON target.snapshot_date = source.snapshot_date
           AND target.store_id = source.store_id
           AND target.product_id = source.product_id

        WHEN MATCHED THEN
          UPDATE SET
            target.on_hand_qty = source.on_hand_qty,
            target.reorder_point = source.reorder_point,
            target.source_system = source.source_system,
            target.updated_timestamp = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
          INSERT (
            snapshot_date,
            store_id,
            product_id,
            on_hand_qty,
            reorder_point,
            source_system,
            created_timestamp,
            updated_timestamp
          )
          VALUES (
            source.snapshot_date,
            source.store_id,
            source.product_id,
            source.on_hand_qty,
            source.reorder_point,
            source.source_system,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
    """)

    logger.info("Inventory UPSERT Completed Successfully")

except Exception as e:
    logger.error("Error in Inventory UPSERT")
    logger.error(str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.inventory_snapshot