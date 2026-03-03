# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_omnichannel_dbx MANAGED LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_omnichannel_dbx;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA retail_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_gold.daily_sales_summary (
# MAGIC
# MAGIC     sales_date DATE,
# MAGIC     total_transactions BIGINT,
# MAGIC     total_quantity BIGINT,
# MAGIC     total_gross_amount DECIMAL(18,2),
# MAGIC     total_discount_amount DECIMAL(18,2),
# MAGIC     total_net_amount DECIMAL(18,2),
# MAGIC
# MAGIC     channel STRING,  -- POS or ECOMMERCE
# MAGIC     created_timestamp TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (sales_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_gold.product_sales_performance (
# MAGIC
# MAGIC     sales_date DATE,
# MAGIC     product_id STRING,
# MAGIC     product_name STRING,
# MAGIC     category STRING,
# MAGIC     brand STRING,
# MAGIC
# MAGIC     total_quantity BIGINT,
# MAGIC     total_revenue DECIMAL(18,2),
# MAGIC
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (sales_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_gold.customer_360 (
# MAGIC
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     city STRING,
# MAGIC     loyalty_status STRING,
# MAGIC
# MAGIC     total_orders BIGINT,
# MAGIC     total_spend DECIMAL(18,2),
# MAGIC
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_gold.inventory_alerts (
# MAGIC
# MAGIC     snapshot_date DATE,
# MAGIC     store_id STRING,
# MAGIC     product_id STRING,
# MAGIC     on_hand_qty INT,
# MAGIC     reorder_point INT,
# MAGIC     is_below_reorder STRING,
# MAGIC
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (snapshot_date);