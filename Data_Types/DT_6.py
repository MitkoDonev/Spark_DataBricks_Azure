# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

webSalesDF = spark.read.table("web_sales")
webSalesDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, collect_set, size, explode, array_contains, create_map, trim

# COMMAND ----------

customerItemDF = webSalesDF.groupBy(col("ws_bill_customer_sk")).agg(collect_set(col("ws_item_sk")).alias("itemList"))
customerItemDF.printSchema()

# COMMAND ----------

display(customerItemDF)

# COMMAND ----------

customerItemDF.select("*", size(col("itemList")).alias("total_items")).show(20)

# COMMAND ----------

customerItemDF.select("*", size(col("itemList")).alias("total_items"), explode(col("itemList"))).show(5)

# COMMAND ----------

customerItemDF.select("*", size(col("itemList")).alias("total_items"), array_contains(col("itemList"), 13910).alias("contains")).show(20)

# COMMAND ----------

customerItemDF.select(col("itemList")[0].alias("zero_index")).show(20)

# COMMAND ----------

customerItemDF.select(col("itemList").getItem(1).alias("get_first_index")).show(20)

# COMMAND ----------

itemDF = spark.read.table("retailer_db.item")
itemDF.printSchema()

# COMMAND ----------

itemInfoDF = itemDF.select(col("i_item_sk"), create_map(trim(col("i_category")), trim(col("i_product_name"))).alias("category"))
itemInfoDF.printSchema()

# COMMAND ----------

itemInfoDF.show(20)

# COMMAND ----------

itemInfoDF.select("*", explode(col("category"))).withColumnRenamed("key", "category_desc").withColumnRenamed("value", "product_name").show(20)
