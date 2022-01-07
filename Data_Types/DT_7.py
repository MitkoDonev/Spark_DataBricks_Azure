# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

from pyspark.sql.functions import sequence, col, create_map

# COMMAND ----------

customerEmailDF = spark.sql("select c_salutation, c_first_name, c_last_name, c_email_address, c_birth_year, c_birth_month, c_preferred_cust_flag from customer")
customerEmailDF.printSchema()

# COMMAND ----------

rowsWithValuesDF = customerEmailDF.na.drop("all")
customerEmailDF.count() - rowsWithValuesDF.count()

# COMMAND ----------

cleanDF = rowsWithValuesDF.na.drop("any")
display(cleanDF)

# COMMAND ----------

customerEmailDF.count() - cleanDF.count()

# COMMAND ----------

nullFilled = rowsWithValuesDF.na.fill("A Wonderful World")
display(nullFilled)
