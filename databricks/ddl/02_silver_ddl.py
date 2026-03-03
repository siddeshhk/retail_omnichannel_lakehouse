# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_omnichannel_dbx MANAGED LOCATION 'abfss://retailer@retaileromnichannelgen2.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_omnichannel_dbx;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA retail_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists retail_silver.dim_customers

# COMMAND ----------

# DBTITLE 1,dim_customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retail_silver.dim_customers (
# MAGIC     customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     customer_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     region STRING,
# MAGIC     city STRING,
# MAGIC     signup_date DATE,
# MAGIC     status STRING,
# MAGIC     hash_value STRING NOT NULL,
# MAGIC     effective_start_date DATE NOT NULL,
# MAGIC     effective_end_date DATE NOT NULL,
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     created_timestamp TIMESTAMP NOT NULL,
# MAGIC     updated_timestamp TIMESTAMP NOT NULL
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_silver.dim_products (
# MAGIC     product_id STRING,
# MAGIC     sku STRING,
# MAGIC     product_name STRING,
# MAGIC     category STRING,
# MAGIC     brand STRING,
# MAGIC     unit_price DECIMAL(10,2),
# MAGIC     is_discontinued STRING,
# MAGIC     hash_value STRING,
# MAGIC     effective_start_date DATE,
# MAGIC     effective_end_date DATE,
# MAGIC     is_current STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_silver.inventory_snapshot (
# MAGIC     snapshot_date DATE,
# MAGIC     store_id STRING,
# MAGIC     product_id STRING,
# MAGIC     on_hand_qty INT,
# MAGIC     reorder_point INT,
# MAGIC     source_system STRING,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists retail_silver.pos_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_silver.pos_sales (
# MAGIC
# MAGIC     transaction_id STRING,
# MAGIC     transaction_ts TIMESTAMP,
# MAGIC     transaction_date DATE,
# MAGIC     store_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10,2), 
# MAGIC     discount_pct DECIMAL(5,2),
# MAGIC     gross_amount DECIMAL(12,2),
# MAGIC     discount_amount DECIMAL(12,2),
# MAGIC     net_amount DECIMAL(12,2),
# MAGIC     payment_type STRING,
# MAGIC     source_system STRING,
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_silver.ecommerce_orders (
# MAGIC
# MAGIC     order_id STRING,
# MAGIC     order_ts TIMESTAMP,
# MAGIC     order_date DATE,
# MAGIC
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(10,2),
# MAGIC     discount_pct DECIMAL(5,2),
# MAGIC
# MAGIC     gross_amount DECIMAL(12,2),
# MAGIC     discount_amount DECIMAL(12,2),
# MAGIC     net_amount DECIMAL(12,2),
# MAGIC
# MAGIC     shipping_city STRING,
# MAGIC     shipping_region STRING,
# MAGIC     delivery_type STRING,
# MAGIC
# MAGIC     order_status STRING,
# MAGIC     source_system STRING,
# MAGIC
# MAGIC     created_timestamp TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA