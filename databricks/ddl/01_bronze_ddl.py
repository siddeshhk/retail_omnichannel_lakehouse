# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_omnichannel_dbx MANAGED LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_omnichannel_dbx;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA retail_bronze;

# COMMAND ----------

# DBTITLE 1,pos_sales
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_bronze.pos_sales (
# MAGIC     transaction_id STRING,
# MAGIC     transaction_ts STRING,
# MAGIC     store_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity STRING,
# MAGIC     unit_price STRING,
# MAGIC     discount_pct STRING,
# MAGIC     payment_type STRING,
# MAGIC     source_system STRING,
# MAGIC     -- Audit Columns
# MAGIC     load_timestamp TIMESTAMP,
# MAGIC     ingestion_date DATE,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/bronze/pos_sales'
# MAGIC PARTITIONED BY (ingestion_date);

# COMMAND ----------

# DBTITLE 1,customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_bronze.customers (
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     region STRING,
# MAGIC     city STRING,
# MAGIC     signup_date STRING,
# MAGIC     status STRING,
# MAGIC     snapshot_date STRING,
# MAGIC     -- Audit Columns
# MAGIC     load_timestamp TIMESTAMP,
# MAGIC     ingestion_date DATE,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/bronze/customers'
# MAGIC PARTITIONED BY (ingestion_date);

# COMMAND ----------

# DBTITLE 1,ecommerce_orders
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_bronze.ecommerce_orders (
# MAGIC     order_id STRING,
# MAGIC     order_ts STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity STRING,
# MAGIC     unit_price STRING,
# MAGIC     discount_pct STRING,
# MAGIC     shipping STRUCT<
# MAGIC         city: STRING,
# MAGIC         region: STRING,
# MAGIC         delivery_type: STRING
# MAGIC     >,
# MAGIC     order_status STRING,
# MAGIC     source_system STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     load_timestamp TIMESTAMP,
# MAGIC     ingestion_date DATE,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/bronze/ecommerce_orders'
# MAGIC PARTITIONED BY (ingestion_date);

# COMMAND ----------

# DBTITLE 1,inventory_snapshot
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_bronze.inventory_snapshot (
# MAGIC     snapshot_date STRING,
# MAGIC     store_id STRING,
# MAGIC     product_id STRING,
# MAGIC     on_hand_qty STRING,
# MAGIC     reorder_point STRING,
# MAGIC     source_system STRING,
# MAGIC     -- Audit Columns
# MAGIC     load_timestamp TIMESTAMP,
# MAGIC     ingestion_date DATE,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/bronze/inventory_snapshot'
# MAGIC PARTITIONED BY (ingestion_date);

# COMMAND ----------

# DBTITLE 1,product
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_bronze.products (
# MAGIC     product_id STRING,
# MAGIC     sku STRING,
# MAGIC     product_name STRING,
# MAGIC     category STRING,
# MAGIC     brand STRING,
# MAGIC     unit_price STRING,
# MAGIC     is_discontinued STRING,
# MAGIC     snapshot_date STRING,
# MAGIC      -- Audit Columns
# MAGIC     load_timestamp TIMESTAMP,
# MAGIC     ingestion_date DATE,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net/bronze/product'
# MAGIC PARTITIONED BY (ingestion_date);