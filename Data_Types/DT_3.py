# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

from pyspark.sql.functions import col, length, trim, lower, upper, initcap

# COMMAND ----------

itemDF = spark.read.table("retailer_db.item")
itemDF.printSchema()

# COMMAND ----------

resultDF = itemDF.filter(col('i_item_desc').contains("young"))
display(resultDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(i_item_id) from item where i_item_desc like "%young%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select length(i_color) from item

# COMMAND ----------

itemDF.select(length(col("i_color"))).show(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(i_color) from item

# COMMAND ----------

itemDF.filter(trim(col("i_color")) == "pink").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from item where trim(i_color) = 'pink'

# COMMAND ----------

# MAGIC %sql
# MAGIC select i_item_desc,
# MAGIC initcap(i_item_desc),
# MAGIC lower(i_item_desc),
# MAGIC upper(i_item_desc)
# MAGIC from item
# MAGIC limit 20

# COMMAND ----------

itemDF.select(col("i_item_desc"), initcap(col("i_item_desc")), lower(col("i_item_desc")), upper(col("i_item_desc"))).show(20)
