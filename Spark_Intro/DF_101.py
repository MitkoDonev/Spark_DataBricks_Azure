# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer

# COMMAND ----------

customer_df = spark.read.option("header", "true").csv("/FileStore/tables/retailer/data/customer.csv")
customer_df.count()

# COMMAND ----------

customer_df1 = spark.read.option("header", "true").format("csv").load("/FileStore/tables/retailer/data/customer.csv")
customer_df1.count()

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/retailer/data/customer.dat

# COMMAND ----------

customer_df2 = spark.read.option("header", "true").option("sep", "|").format("csv").load("/FileStore/tables/retailer/data/customer.dat")
customer_df2.count()

# COMMAND ----------

customer_df.columns
len(customer_df.columns)

# COMMAND ----------

customerBirthDay = customer_df.select("c_customer_id", "c_first_name", "c_last_name", "c_birth_year", "c_birth_month", "c_birth_day")
customerBirthDay.show()

# COMMAND ----------

from pyspark.sql.functions import col, column

customerWithBirthDate = customer_df.select(col("c_customer_id"), col("c_first_name"), column("c_last_name"))
customerWithBirthDate.show()
