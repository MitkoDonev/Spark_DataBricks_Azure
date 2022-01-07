# Databricks notebook source
from pyspark.sql.functions import col, round

# COMMAND ----------

webSalesDF = spark.read.table("retailer_db.web_sales")
webSalesDF.printSchema()

# COMMAND ----------

salesStatsDF = webSalesDF.select("ws_order_number", "ws_item_sk", "ws_quantity", "ws_net_paid", "ws_net_profit", "ws_wholesale_cost")
salesStatsDF.show(20)

# COMMAND ----------

salesPerformance = salesStatsDF.withColumn("expected_net_paid", round(col("ws_quantity") * col("ws_wholesale_cost"), 2)).withColumn("calculated_profit", round(col("ws_net_paid") - col("expected_net_paid"), 2)).withColumn("unit_price", round(col("ws_wholesale_cost") / col("ws_quantity"), 2))
salesPerformance.show(20)

# COMMAND ----------

salesPerformance.describe()
