# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, LongType

schema = StructType([
    StructField('ib_lower_band_sk', LongType(), True),
    StructField('ib_lower_bound', IntegerType(), True),
    StructField('ib_upper_bound', IntegerType(), True)
])

# COMMAND ----------

incomeBand_df = spark.read.schema(schema).option("sep", "|").csv('/FileStore/tables/retailer/data/income_band.dat')
incomeBand_df.show()

# COMMAND ----------

incomeBandIncomeGroups = incomeBand_df.withColumn("isFirstIncomeGroup", incomeBand_df['ib_upper_bound'] <= 60000).withColumn("isSecondIncomeGroup", (incomeBand_df['ib_upper_bound'] >= 60000) & (incomeBand_df['ib_upper_bound'] <= 120000)).withColumn('isThirdIncomeGroup', (incomeBand_df['ib_upper_bound'] > 120000) & (incomeBand_df['ib_upper_bound'] <= 200000))

# COMMAND ----------

incomeBandIncomeGroups.show()

# COMMAND ----------

incomeBandIncomeGroups.printSchema()

# COMMAND ----------

incomeClasses = incomeBandIncomeGroups.withColumnRenamed('isFirstIncomeGroup', "isLowIncomeClass").withColumnRenamed('isSecondIncomeGroup', "isIntermediateIncomeClass").withColumnRenamed('isThirdIncomeGroup', "isHighIncomeClass")
incomeClasses.printSchema()

# COMMAND ----------


