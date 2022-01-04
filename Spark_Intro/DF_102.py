# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer

# COMMAND ----------

customer_df = spark.read.option("header", "true").csv("/FileStore/tables/retailer/data/customer.csv")

# COMMAND ----------

customerBirthDay = customer_df.select("c_customer_id", "c_first_name", "c_last_name", "c_birth_year", "c_birth_month", "c_birth_day")
customerBirthDay.show()

# COMMAND ----------

customerBirthDay.printSchema()

# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

df_with_types = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/customer.csv")
df_with_types.printSchema()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

customer_address_df_bug = spark.read.option("header", "true").option('sep', '|').option('inferSchema', 'true').csv("/FileStore/tables/retailer/data/customer_address.dat")
customer_address_df_bug.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType

schema = StructType([
    StructField('ca_address_sk', LongType(), True),
    StructField('ca_address_id', StringType(), True),
    StructField('ca_street_number', StringType(), True),
    StructField('ca_gmt_offset', DecimalType(5,2), True),
                    ])

# COMMAND ----------

customer_address_df = spark.read.option("header", "true").option('sep', '|').schema(schema).csv("/FileStore/tables/retailer/data/customer_address.dat")
customer_address_df_bug.printSchema()
