# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest Orders
# MAGIC Task 1 of `sample_transform_job`.
# MAGIC
# MAGIC Stands in for the upstream order feed: generates a batch of synthetic
# MAGIC order events and lands them in the raw orders table. Downstream
# MAGIC aggregation happens in `aggregate_sales`.

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_sandbox")
dbutils.widgets.text("schema", "mcp_jobs_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_table = f"{catalog}.{schema}.orders_raw"

print(f"Landing raw orders in {raw_table}")

# COMMAND ----------

from datetime import datetime, timedelta
import random

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Seeded so successive runs land the same batch — makes the downstream numbers
# reproducible when we are iterating on the transform.
random.seed(42)

num_records = 5000
regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]
products = ["widget-a", "widget-b", "gadget-x", "gadget-y", "service-z"]
statuses = ["completed", "pending", "cancelled", "refunded"]
channels = ["web", "partner", "field-sales"]

data = [
    (
        f"order-{i:05d}",
        random.choice(regions),
        random.choice(products),
        random.choice(channels),
        random.choice(statuses),
        round(random.uniform(10.0, 500.0), 2),
        random.randint(1, 20),
        # Promotions run on most orders; a discount of 0 means list price.
        round(random.choice([0.0, 0.0, 0.05, 0.1, 0.15, 0.25]), 2),
        datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 8760)),
    )
    for i in range(num_records)
]

schema_def = StructType([
    StructField("order_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("status", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("discount_pct", DoubleType(), False),
    StructField("order_date", TimestampType(), False),
])

orders_df = spark.createDataFrame(data, schema=schema_def)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

(
    orders_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(raw_table)
)

print(f"Wrote {orders_df.count()} raw orders to {raw_table}")

# COMMAND ----------

display(spark.table(raw_table).limit(20))
