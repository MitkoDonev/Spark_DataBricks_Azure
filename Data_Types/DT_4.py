# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

singleItemSoldDateDF = spark.sql("select ws_order_number, ws_item_sk item_id, d_date sold_date from web_sales inner join date_dim on ws_sold_date_sk = d_date_sk where ws_item_sk = 4591 and ws_order_number = 1")
display(singleItemSoldDateDF)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit, unix_timestamp, from_unixtime
from pyspark.sql.types import TimestampType

# COMMAND ----------

dateDF = singleItemSoldDateDF.withColumn("date_1", to_date(lit("1999-07-03"))).withColumn("date_2", to_date(lit("1999/07/03"), "yyyy/MM/dd")).withColumn("date_3", to_date(lit("23.07.1999"), "dd.MM.yyyy")).withColumn("date_4", to_date(lit("1999.07.03"), "yyyy.MM.dd")).withColumn("date_5", to_date(lit("1999.07.03 20:31:53"), "yyyy.MM.dd HH:mm:ss")).withColumn("date_6", to_date(lit("1999.07.03 20:31:53 PM"), "yyyy.MM.dd HH:mm:ss a")).withColumn("date_7", unix_timestamp(lit("1999.07.03 20:31:53"), "yyyy.MM.dd HH:mm:ss")).withColumn("date_8", from_unixtime(col("date_7"), "dd.MM.yyyy")).withColumn("date_9", unix_timestamp(lit("1999.07.23 20:21:53 PM"), "yyyy.MM.dd HH:mm:ss a").cast(TimestampType()))

# COMMAND ----------

display(dateDF)
