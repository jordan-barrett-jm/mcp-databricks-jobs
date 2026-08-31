# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Sample Transform Job
# MAGIC Simple data transformation on synthetic data.
# MAGIC Deployed via Declarative Automation Bundles, triggered by MCP server.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta
import random

# COMMAND ----------

# Generate sample order data
num_records = 1000
regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]
products = ["widget-a", "widget-b", "gadget-x", "gadget-y", "service-z"]
statuses = ["completed", "pending", "cancelled", "refunded"]

data = [
    (
        f"order-{i:05d}",
        random.choice(regions),
        random.choice(products),
        random.choice(statuses),
        round(random.uniform(10.0, 500.0), 2),
        random.randint(1, 20),
        datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 8760)),
    )
    for i in range(num_records)
]

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("status", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_date", TimestampType(), False),
])

orders_df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# Transform: compute revenue, filter completed orders, aggregate by region
transformed = (
    orders_df
    .withColumn("revenue", F.col("unit_price") * F.col("quantity"))
    .filter(F.col("status") == "completed")
    .groupBy("region", "product")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_order_value"),
        F.max("order_date").alias("latest_order"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------

# Display results (visible in job run output)
transformed.show(truncate=False)
print(f"\nTransformation complete: {transformed.count()} region-product combinations processed.")
print(f"Total revenue: ${transformed.agg(F.sum(\"total_revenue\")).first()[0]:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job completed successfully ✅