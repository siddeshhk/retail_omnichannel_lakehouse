# Databricks notebook source
# MAGIC %md
# MAGIC * **Notebook**: 01_bronze_ecom_orders_ingestion
# MAGIC * **Layer**: Bronze
# MAGIC * **Table**: `retail_bronze.ecommerce_orders`
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

inventory_schema = StructType([
        StructField("snapshot_date", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("on_hand_qty", StringType(), True),
        StructField("reorder_point", StringType(), True),
        StructField("source_system", StringType(), True)
    ])

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import traceback

try:
    log_message("INFO", "Starting Bronze Ingestion for inventory_snapshot")
    
    df_raw = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(inventory_schema) \
        .load(SOURCE_PATH)

    source_count = df_raw.count()
    log_message("INFO", f"Source records count: {source_count}")
    
    df_casted = df_raw.select(
        col("snapshot_date").cast("string"),
        col("store_id").cast("string"),
        col("product_id").cast("string"),
        col("on_hand_qty").cast("string"),
        col("reorder_point").cast("string"),
        col("source_system").cast("string")
    )
    
    df_bronze = df_casted \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("source_file_name", col("_metadata.file_path"))
    
    final_columns = [
        "snapshot_date",
        "store_id",
        "product_id",
        "on_hand_qty",
        "reorder_point",
        "source_system",
        "load_timestamp",
        "ingestion_date",
        "source_file_name"
    ]

    df_bronze = df_bronze.select(final_columns)
    
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)

    log_message("INFO", "Inventory_snapshot Orders successfully written to Bronze table")

except Exception as e:
    log_message("ERROR", "Bronze ingestion failed")
    log_message("ERROR", str(e))
    log_message("ERROR", traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_bronze.inventory_snapshot