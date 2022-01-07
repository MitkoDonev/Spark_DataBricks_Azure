# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

from pyspark.sql.functions import lit, col, trim

# COMMAND ----------

householdDemographicsDF = spark.read.schema("hd_demo_sk LONG, hd_income_band_sk LONG, hd_buy_potential STRING, hd_dep_count INT, hd_behicle_count INT").option("header", "true").option("inferShema", "true").option("sep", "|").csv("/FileStore/tables/retailer/data/household_demographics.dat")
householdDemographicsDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

# COMMAND ----------

schema = StructType([
    StructField("hd_demo_sk", LongType(), True),
    StructField("hd_income_band_sk", LongType(), True),
    StructField("hd_buy_potential", StringType(), True),
    StructField("hd_dep_count", IntegerType(), True),
    StructField("hd_behicle_count", IntegerType(), True),
])

# COMMAND ----------

householdDemographicsWithSchemDF = spark.read.schema(schema).option("header", "true").option("inferShema", "true").option("sep", "|").csv("/FileStore/tables/retailer/data/household_demographics.dat")
householdDemographicsWithSchemDF.printSchema()

# COMMAND ----------

householdDemographicsDF.select("*", lit("house").name("new_column_house"), lit(3).name("new_integer_column")).show()

# COMMAND ----------

householdDemographicsDF.filter(col('hd_income_band_sk') == 2).filter(col('hd_behicle_count') > 3).orderBy(col('hd_behicle_count').desc()).show(5)

# COMMAND ----------

householdDemographicsDF.select("hd_buy_potential").distinct().show(10)

# COMMAND ----------

isBuyPotentialHigh = trim(col('hd_buy_potential')) == "5001-10000"

# COMMAND ----------

df = householdDemographicsDF.withColumn("high_potential_buy", isBuyPotentialHigh)
df.printSchema()
df.select("*").orderBy(col('high_potential_buy').desc()).show(20)

# COMMAND ----------

df.filter(col("hd_behicle_count") > 3).orderBy(col("hd_behicle_count")).show(20)
