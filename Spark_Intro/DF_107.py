# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

from pyspark.sql.functions import sum, round

# COMMAND ----------

dateDimDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/date_dim.dat")
display(dateDimDF)

# COMMAND ----------

storeSalesDF = spark.read.option("header", "true").option('sep', "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/store_sales.dat")
display(storeSalesDF)

# COMMAND ----------

itemDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/item.dat")
display(itemDF)

# COMMAND ----------

itemManufacturerID = 128
salesMonth = 11

# COMMAND ----------

first_condition = storeSalesDF['ss_sold_date_sk'] == dateDimDF['d_date_sk']
second_condtion = itemDF['i_item_sk'] == storeSalesDF['ss_item_sk']

dateDimSalesStoreDF = dateDimDF.join(storeSalesDF, first_condition, "inner").join(itemDF, second_condtion, "inner")
filterManufactorerIdsDF = dateDimSalesStoreDF.filter(dateDimSalesStoreDF['i_manufact_id'] == itemManufacturerID).filter(dateDimSalesStoreDF['d_moy'] == salesMonth)

groupedDF = filterManufactorerIdsDF.groupBy("d_year", "d_moy", "i_brand", "i_brand_id", "i_item_sk").agg(round(sum('ss_ext_sales_price'), 2))

# COMMAND ----------

formated_result_DF = groupedDF.withColumnRenamed("d_year", "year").withColumnRenamed("d_moy", "month").withColumnRenamed("i_brand", "brand").withColumnRenamed("i_brand_id", "brand_id").withColumnRenamed("i_item_sk", "item_id").withColumnRenamed("round(sum(ss_ext_sales_price), 2)", "total_price")

display(formated_result_DF)
