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
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, max
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("silver_ecommerce_orders")
logger.info("Silver ecommerce_orders Notebook Started")

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
    logger.info("Reading Bronze Ecommerce Orders Table")

    df_bronze = spark.table(source_table)

    bronze_count = df_bronze.count()
    logger.info(f"Bronze Ecommerce Orders Count: {bronze_count}")

    if bronze_count == 0:
        raise Exception("No records found in Bronze ecommerce_orders")

    df_source = df_bronze

except Exception as e:
    logger.error("Error in Bronze Read")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Flattening Shipping Struct + Standardizing Columns")

    df_source = df_source \
        .withColumn("order_ts", to_timestamp(col("order_ts"))) \
        .withColumn("order_date", to_date(col("order_ts"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("order_status", trim(col("order_status"))) \
        .withColumn("source_system", trim(col("source_system"))) \
        .withColumn("shipping_city", col("shipping.city")) \
        .withColumn("shipping_region", col("shipping.region")) \
        .withColumn("delivery_type", col("shipping.delivery_type"))

    # Cast numeric columns
    df_source = df_source \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
        .withColumn("discount_pct", col("discount_pct").cast("decimal(5,2)"))

    # Derived financial columns with explicit decimal type to avoid schema mismatch
    df_source = df_source \
        .withColumn("gross_amount", (col("quantity") * col("unit_price")).cast("decimal(21,6)")) \
        .withColumn("discount_amount",
                    (col("gross_amount") * (col("discount_pct")/100)).cast("decimal(21,6)")) \
        .withColumn("net_amount",
                    (col("gross_amount") - col("discount_amount")).cast("decimal(21,6)"))

    logger.info("Applying Mandatory Validation")

    df_valid = df_source.filter(
        col("order_id").isNotNull() &
        col("order_ts").isNotNull() &
        col("product_id").isNotNull()
    )

    logger.info(f"Valid Ecommerce Orders Count: {df_valid.count()}")

except Exception as e:
    logger.error("Error in Flattening / Standardization")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Applying Deduplication")

    window_spec = Window.partitionBy(
        "order_id", "product_id"
    ).orderBy(
        col("ingestion_date").desc(),
        col("load_timestamp").desc()
    )

    df_deduplicated = df_valid \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num", "ingestion_date", "load_timestamp", "source_file_name", "shipping")

    logger.info(f"Deduplicated Count: {df_deduplicated.count()}")

except Exception as e:
    logger.error("Error in Deduplication")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Writing Ecommerce Orders to Silver (Append Mode)")

    # Align decimal precision/scale to match target table schema
    df_write = df_deduplicated \
        .withColumn("gross_amount", col("gross_amount").cast("decimal(12,2)")) \
        .withColumn("discount_amount", col("discount_amount").cast("decimal(12,2)")) \
        .withColumn("net_amount", col("net_amount").cast("decimal(12,2)")) \
        .withColumn("created_timestamp", current_timestamp())

    df_write.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    logger.info("Ecommerce Orders Silver Load Completed Successfully")

except Exception as e:
    logger.error("Error writing Ecommerce Orders to Silver")
    logger.error(str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.ecommerce_orders