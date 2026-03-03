# Databricks notebook source
# MAGIC %md
# MAGIC * **Notebook**: 01_bronze_pos_sales_ingestion
# MAGIC * **Layer**: Bronze
# MAGIC * **Table**: `retail_bronze.pos_sales`
# MAGIC * **Author**: Siddesh HK
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import traceback

def log_message(level, message):
    print(f"[{level}] {datetime.now()} - {message}")

# COMMAND ----------

try:
    # Create widgets
    dbutils.widgets.text("source_checkpoint_folder", "")
    dbutils.widgets.text("target_table", "")

    # Get widget values
    source_checkpoint_folder = dbutils.widgets.get("source_checkpoint_folder")
    target_table = dbutils.widgets.get("target_table")

    if source_checkpoint_folder == "" or target_table == "":
        raise Exception("Source folder or Target table or checkpoint folder parameters not passed")

except Exception as e:
    log_message("ERROR", "Error in Bronze parameters read")
    log_message("ERROR", str(e))
    log_message("ERROR", traceback.format_exc())
    raise

# COMMAND ----------

# Configurations
SOURCE_PATH = f"abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/landing/{source_checkpoint_folder}/"
TARGET_TABLE = f"{target_table}"
CHECKPOINT_PATH = f"abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/checkpoints/bronze/{source_checkpoint_folder}/"

# COMMAND ----------

from pyspark.sql.types import *

pos_sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_ts", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_pct", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("source_system", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import traceback

try:
    log_message("INFO", "Starting Bronze Ingestion for POS Sales (CSV)")

    # Read CSV files (Unity Catalog compatible)
    df_raw = spark.read \
        .format("csv") \
        .schema(pos_sales_schema) \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .load(SOURCE_PATH)

    source_count = df_raw.count()
    log_message("INFO", f"Source records count: {source_count}")

    # Add Audit Columns using Unity Catalog supported metadata
    df_bronze = df_raw \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("source_file_name", col("_metadata.file_path"))

    # Basic Data Validation
    invalid_count = df_bronze.filter(col("transaction_id").isNull()).count()

    if invalid_count > 0:
        log_message("WARNING", f"Found {invalid_count} records with NULL transaction_id")

    # Select only required columns (VERY IMPORTANT)
    final_columns = [
        "transaction_id",
        "transaction_ts",
        "store_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_type",
        "source_system",
        "load_timestamp",
        "ingestion_date",
        "source_file_name"
    ]

    df_bronze = df_bronze.select(final_columns)

    # Write to Bronze Table
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)

    log_message("INFO", "Data successfully written to Bronze table")

except Exception as e:
    log_message("ERROR", "Bronze ingestion failed")
    log_message("ERROR", str(e))
    log_message("ERROR", traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_bronze.pos_sales