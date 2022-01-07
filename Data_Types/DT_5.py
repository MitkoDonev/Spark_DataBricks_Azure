# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

webSalesDF = spark.read.table("retailer_db.web_sales")

# COMMAND ----------

from pyspark.sql.functions import col, struct, expr

# COMMAND ----------

itemQuantityDF = webSalesDF.select(col("ws_bill_customer_sk").alias("customer_id"), struct(col("ws_item_sk").alias("item_id"), col("ws_quantity").alias("quantity")).alias("item_quantity"))
itemQuantityDF.printSchema()

# COMMAND ----------

display(itemQuantityDF)

# COMMAND ----------

itemQuantityDF.select("customer_id", "item_quantity.item_id", "item_quantity.quantity").show()

# COMMAND ----------

itemQuantityDF.select(col("customer_id"), col("item_quantity").getField("quantity").alias("quantity")).show(20)
