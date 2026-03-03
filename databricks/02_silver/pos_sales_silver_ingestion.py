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
from pyspark.sql.window import Window
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("SILVER_POS_SALES")
logger.info("Silver POS Sales Notebook Started")

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
    logger.info("Reading Bronze POS Sales Table")

    df_bronze = spark.table(source_table)

    bronze_count = df_bronze.count()
    logger.info(f"Bronze POS Sales Count: {bronze_count}")

    if bronze_count == 0:
        raise Exception("No records found in Bronze pos_sales")

    df_source = df_bronze

except Exception as e:
    logger.error("Error in Bronze Read")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Standardizing POS Sales Columns")

    df_source = df_source \
        .withColumn("transaction_ts", to_timestamp(col("transaction_ts"))) \
        .withColumn("transaction_date", to_date(col("transaction_ts"))) \
        .withColumn("store_id", trim(col("store_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("payment_type", trim(col("payment_type"))) \
        .withColumn("source_system", trim(col("source_system")))

    # Numeric casting
    df_source = df_source \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
        .withColumn("discount_pct", col("discount_pct").cast("decimal(5,2)"))

    # Derived columns
    df_source = df_source \
        .withColumn("gross_amount", col("quantity") * col("unit_price")) \
        .withColumn("discount_amount",
                    col("gross_amount") * (col("discount_pct")/100)) \
        .withColumn("net_amount",
                    col("gross_amount") - col("discount_amount"))

    logger.info("Applying Mandatory Validation")

    df_valid = df_source.filter(
        col("transaction_id").isNotNull() &
        col("transaction_ts").isNotNull() &
        col("store_id").isNotNull() &
        col("product_id").isNotNull()
    )

    logger.info(f"Valid POS Sales Count: {df_valid.count()}")

except Exception as e:
    logger.error("Error in Standardization / Validation")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Applying Deduplication")

    window_spec = Window.partitionBy(
        "transaction_id", "product_id"
    ).orderBy(
        col("ingestion_date").desc(),
        col("load_timestamp").desc()
    )

    df_deduplicated = df_valid \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num", "ingestion_date", "load_timestamp", "source_file_name")

    logger.info(f"Deduplicated Count: {df_deduplicated.count()}")

except Exception as e:
    logger.error("Error in Deduplication")
    logger.error(str(e))
    raise

# COMMAND ----------

try:
    logger.info("Writing POS Sales to Silver (Append Mode)")

    # Ensure decimal precision/scale matches target table schema
    target_schema = spark.table(target_table).schema
    gross_amount_type = [f.dataType for f in target_schema if f.name == "gross_amount"]
    discount_amount_type = [f.dataType for f in target_schema if f.name == "discount_amount"]
    net_amount_type = [f.dataType for f in target_schema if f.name == "net_amount"]

    df_to_write = df_deduplicated.withColumn("created_timestamp", current_timestamp())

    if gross_amount_type:
        df_to_write = df_to_write.withColumn("gross_amount", col("gross_amount").cast(gross_amount_type[0]))
    if discount_amount_type:
        df_to_write = df_to_write.withColumn("discount_amount", col("discount_amount").cast(discount_amount_type[0]))
    if net_amount_type:
        df_to_write = df_to_write.withColumn("net_amount", col("net_amount").cast(net_amount_type[0]))

    df_to_write.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    logger.info("POS Sales Silver Load Completed Successfully")

except Exception as e:
    logger.error("Error writing POS Sales to Silver")
    logger.error(str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.pos_sales