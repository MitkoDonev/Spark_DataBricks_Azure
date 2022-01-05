# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

import datetime
from pyspark.sql.functions import concat, col
from pyspark.sql.types import StringType

# COMMAND ----------

customerDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/customer.dat")
customerDF.printSchema()

# COMMAND ----------

def getCustomerBirthDate(year, month, day):
    return f"{year}/{month}/{day}"

# COMMAND ----------

from pyspark.sql.functions import udf

getCustomerBirthDate_udf = udf(getCustomerBirthDate, StringType())

# COMMAND ----------

select_customerDF = customerDF.select(getCustomerBirthDate_udf(col('c_birth_year'), col('c_birth_month'), col('c_birth_day')).alias("birth_date"), "c_customer_sk", (concat(customerDF['c_first_name'], customerDF['c_last_name']).alias("full_name")), "c_birth_year", "c_birth_month", "c_birth_day")
format_selectedCustomerDF = select_customerDF.withColumnRenamed("c_customer_sk", "customer_id").withColumnRenamed("c_birth_year", "birth_year").withColumnRenamed("c_birth_month", "birth_month").withColumnRenamed("c_birth_day", "birth_day")
display(format_selectedCustomerDF)

# COMMAND ----------

customerDF.createOrReplaceTempView("vCustomer")
