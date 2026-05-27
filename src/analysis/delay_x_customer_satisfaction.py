# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

orders = spark.read.table("cat_olist.sch_silver.orders")

# COMMAND ----------

reviews = spark.read.table("cat_olist.sch_silver.reviews")

# COMMAND ----------

orders.join(
    reviews,
    on="order_id",
    how="inner"
)

# COMMAND ----------

reviews.display()

# COMMAND ----------

orders.withColumn(
    "delay_range",
    F.when(F.col("delivery_delay_days") < 0, "Early")
    .when(F.col("delivery_delay_days") == 0, "On time")
    .when(F.col("delivery_delay_days").between(1, 3), "1 to 3 days")
    .when(F.col("delivery_delay_days").between(4, 7), "4 to 7 days")
    .when(F.col("delivery_delay_days") > 7, "More than 7 days")
).groupBy("delay_range").agg(
    F.countDistinct(F.col("order_id")).alias("total_orders"),
).display()
