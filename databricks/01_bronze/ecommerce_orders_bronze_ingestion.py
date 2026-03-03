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

ecommerce_schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("order_ts", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("shipping", StructType([
            StructField("city", StringType(), True),
            StructField("region", StringType(), True),
            StructField("delivery_type", StringType(), True)
        ]), True),
        StructField("order_status", StringType(), True),
        StructField("source_system", StringType(), True)
    ])


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import traceback

try:
    log_message("INFO", "Starting Bronze Ingestion for E-Commerce Orders (JSON)")
    
    df_raw = spark.read \
        .format("json") \
        .schema(ecommerce_schema) \
        .option("multiLine", "false") \
        .load(SOURCE_PATH)

    source_count = df_raw.count()
    log_message("INFO", f"Source records count: {source_count}")
    
    df_casted = df_raw.select(
        col("order_id").cast("string"),
        col("order_ts").cast("string"),
        col("customer_id").cast("string"),
        col("product_id").cast("string"),
        col("quantity").cast("string"),
        col("unit_price").cast("string"),
        col("discount_pct").cast("string"),
        col("shipping"),  # Keep struct as-is
        col("order_status").cast("string"),
        col("source_system").cast("string")
    )
    
    df_bronze = df_casted \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("source_file_name", col("_metadata.file_path"))
    
    invalid_count = df_bronze.filter(col("order_id").isNull()).count()

    if invalid_count > 0:
        log_message("WARNING", f"Found {invalid_count} records with NULL order_id")
    
    final_columns = [
        "order_id",
        "order_ts",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "shipping",
        "order_status",
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

    log_message("INFO", "E-Commerce Orders successfully written to Bronze table")

except Exception as e:
    log_message("ERROR", "Bronze ingestion failed")
    log_message("ERROR", str(e))
    log_message("ERROR", traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_bronze.ecommerce_orders