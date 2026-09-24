# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Aggregate Sales
# MAGIC Task 2 of `sample_transform_job`. Depends on `ingest_orders`.
# MAGIC
# MAGIC Rolls completed orders up to region x product and publishes the result
# MAGIC to the reporting table `sales_by_region`.

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_sandbox")
dbutils.widgets.text("schema", "mcp_jobs_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw_table = f"{catalog}.{schema}.orders_raw"
report_table = f"{catalog}.{schema}.sales_by_region"

print(f"Reading {raw_table} -> writing {report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reporting table contract
# MAGIC The reporting table is declared here rather than inferred from the
# MAGIC DataFrame, so the column types and comments downstream consumers rely on
# MAGIC stay pinned across runs.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {report_table} (
    region          STRING        COMMENT 'Sales region the order was placed in',
    product         STRING        COMMENT 'Product SKU',
    order_count     BIGINT        COMMENT 'Number of completed orders',
    gross_revenue   DECIMAL(12,2) COMMENT 'Revenue at list price',
    net_revenue     DECIMAL(12,2) COMMENT 'Revenue after promotional discounts',
    avg_order_value DOUBLE        COMMENT 'Mean net revenue per completed order',
    latest_order    TIMESTAMP     COMMENT 'Timestamp of the most recent order',
    ingested_at     TIMESTAMP     COMMENT 'When this snapshot was published'
)
USING DELTA
COMMENT 'Completed-order revenue by region and product'
""")

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(raw_table)

priced = (
    orders
    .filter(F.col("status") == "completed")
    .withColumn("gross_revenue", F.col("unit_price") * F.col("quantity"))
    .withColumn("net_revenue", F.col("gross_revenue") * (F.lit(1.0) - F.col("discount_pct")))
)

sales = (
    priced
    .groupBy("region", "product")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("gross_revenue").cast("decimal(12,2)").alias("gross_revenue"),
        F.sum("net_revenue").cast("decimal(12,2)").alias("net_revenue"),
        F.avg("net_revenue").alias("avg_order_value"),
        F.max("order_date").alias("latest_order"),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "region",
        "product",
        "order_count",
        "gross_revenue",
        "net_revenue",
        "avg_order_value",
        "latest_order",
        "ingested_at",
    )
)

# COMMAND ----------

sales.write.mode("overwrite").saveAsTable(report_table)

print(f"Published {sales.count()} region/product rows to {report_table}")

# COMMAND ----------

display(spark.table(report_table).orderBy(F.desc("net_revenue")))
