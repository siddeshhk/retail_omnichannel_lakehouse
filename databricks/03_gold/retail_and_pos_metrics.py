# Databricks notebook source
import logging
from pyspark.sql.functions import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GOLD_LAYER")
logger.info("Gold Layer Notebook Started")

# COMMAND ----------

# DBTITLE 1,Scenario 1
try:
    logger.info("Starting Scenario 1 - Daily Sales Summary")

    df_daily_sales = spark.sql("""

        SELECT
            transaction_date AS sales_date,
            COUNT(DISTINCT transaction_id) AS total_transactions,
            SUM(quantity) AS total_quantity,
            CAST(ROUND(SUM(gross_amount),2) AS DECIMAL(18,2)) AS total_gross_amount,
            CAST(ROUND(SUM(discount_amount),2) AS DECIMAL(18,2)) AS total_discount_amount,
            CAST(SUM(net_amount) AS DECIMAL(18,2)) AS total_net_amount,
            'POS' AS channel,
            CURRENT_TIMESTAMP() AS created_timestamp
        FROM retail_silver.pos_sales
        GROUP BY transaction_date

        UNION ALL

        SELECT
            order_date AS sales_date,
            COUNT(DISTINCT order_id),
            SUM(quantity),
            CAST(ROUND(SUM(gross_amount),2) AS DECIMAL(18,2)),
            CAST(ROUND(SUM(discount_amount),2) AS DECIMAL(18,2)),
            CAST(SUM(net_amount) AS DECIMAL(18,2)),
            'ECOMMERCE',
            CURRENT_TIMESTAMP()
        FROM retail_silver.ecommerce_orders
        GROUP BY order_date

    """)

    logger.info(f"Scenario 1 Record Count: {df_daily_sales.count()}")

    df_daily_sales.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("sales_date") \
        .saveAsTable("retail_gold.daily_sales_summary")

    logger.info("Scenario 1 Completed Successfully")

except Exception as e:
    logger.error("Error in Scenario 1 - Daily Sales Summary")
    logger.error(str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Scenario 2
try:
    logger.info("Starting Scenario 2 - Product Performance")

    df_product_perf = spark.sql("""

        SELECT
            f.transaction_date AS sales_date,
            f.product_id,
            d.product_name,
            d.category,
            d.brand,
            SUM(f.quantity) AS total_quantity,
            CAST(SUM(f.net_amount) AS DECIMAL(18,2)) AS total_revenue,
            CURRENT_TIMESTAMP() AS created_timestamp
        FROM retail_silver.pos_sales f
        JOIN retail_silver.dim_products d
          ON f.product_id = d.product_id
         AND f.transaction_date BETWEEN d.effective_start_date 
                                    AND d.effective_end_date
        GROUP BY
            f.transaction_date,
            f.product_id,
            d.product_name,
            d.category,
            d.brand

    """)

    logger.info(f"Scenario 2 Record Count: {df_product_perf.count()}")

    df_product_perf.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("sales_date") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_gold.product_sales_performance")

    logger.info("Scenario 2 Completed Successfully")

except Exception as e:
    logger.error("Error in Scenario 2 - Product Performance")
    logger.error(str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Scenario 3
try:
    logger.info("Starting Scenario 3 - Customer 360")

    df_customer_360 = spark.sql("""

        WITH combined_sales AS (

            SELECT customer_id,
                   transaction_date AS sales_date,
                   CAST(net_amount AS DECIMAL(18,2)) AS net_amount
            FROM retail_silver.pos_sales

            UNION ALL

            SELECT customer_id,
                   order_date,
                   CAST(net_amount AS DECIMAL(18,2)) AS net_amount
            FROM retail_silver.ecommerce_orders
        )

        SELECT
            c.customer_id,
            c.customer_name,
            c.city,
            c.status,
            COUNT(*) AS total_orders,
            CAST(SUM(s.net_amount) AS DECIMAL(18,2)) AS total_spend,
            CURRENT_TIMESTAMP() AS created_timestamp
        FROM combined_sales s
        JOIN retail_silver.dim_customers c
          ON s.customer_id = c.customer_id
         AND s.sales_date BETWEEN c.effective_start_date 
                              AND c.effective_end_date
        GROUP BY
            c.customer_id,
            c.customer_name,
            c.city,
            c.status

    """)

    logger.info(f"Scenario 3 Record Count: {df_customer_360.count()}")

    df_customer_360.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_gold.customer_360")

    logger.info("Scenario 3 Completed Successfully")

except Exception as e:
    logger.error("Error in Scenario 3 - Customer 360")
    logger.error(str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Scenario 4
try:
    logger.info("Starting Scenario 4 - Inventory Alerts")

    df_inventory_alerts = spark.sql("""

        SELECT
            snapshot_date,
            store_id,
            product_id,
            on_hand_qty,
            reorder_point,
            CASE
                WHEN on_hand_qty < reorder_point THEN 'YES'
                ELSE 'NO'
            END AS is_below_reorder,
            CURRENT_TIMESTAMP() AS created_timestamp
        FROM retail_silver.inventory_snapshot

    """)

    logger.info(f"Scenario 4 Record Count: {df_inventory_alerts.count()}")

    df_inventory_alerts.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .saveAsTable("retail_gold.inventory_alerts")

    logger.info("Scenario 4 Completed Successfully")

except Exception as e:
    logger.error("Error in Scenario 4 - Inventory Alerts")
    logger.error(str(e))
    raise