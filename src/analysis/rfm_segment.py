# Databricks notebook source
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql.functions import col, date_diff, lit, max as _max

# COMMAND ----------

# Format should be YYYY-MM-DD
CURRENT_DATE = "2019-01-01"

# COMMAND ----------

orders = spark.read.table("cat_olist.sch_silver.orders")

# COMMAND ----------

orders.groupby("customer_unique_id").agg(
    date_diff(lit(CURRENT_DATE), _max(col("order_purchase_timestamp"))).alias("recency")
).display()

# COMMAND ----------


