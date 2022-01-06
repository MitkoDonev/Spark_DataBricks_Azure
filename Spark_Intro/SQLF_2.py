# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

customerDF = spark.read.option("header", "true").option("sep", "|").option("inferSchmea", "true").csv("/FileStore/tables/retailer/data/customer.dat")
customerDF.printSchema()

# COMMAND ----------

customerDF.createGlobalTempView("gvCustomer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gvCustomer
