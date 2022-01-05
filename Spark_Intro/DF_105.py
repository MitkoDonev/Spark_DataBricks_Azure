# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, min, max, sum, sumDistinct, avg, mean, round

# COMMAND ----------

customer_df = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/customer.dat")
display(customer_df)

# COMMAND ----------

customer_address_df = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/customer_address.dat")
display(customer_address_df)

# COMMAND ----------

joinExpression = customer_df['c_current_addr_sk'] == customer_address_df['ca_address_sk']

# COMMAND ----------

customerWithAddress_df = customer_df.join(customer_address_df, joinExpression, "inner").select("c_customer_id", "ca_address_sk", "c_salutation", "c_first_name", "c_last_name", "ca_country", "ca_city", "ca_street_name", "ca_zip", "ca_street_number")
display(customerWithAddress_df)

# COMMAND ----------

customer_df.count()

# COMMAND ----------

customer_df.select(count('*').alias("total")).show()

# COMMAND ----------

customer_df.select(count('c_first_name').alias("total_first_name")).show()

# COMMAND ----------

customer_df.filter(customer_df['c_first_name'].isNotNull()).count()

# COMMAND ----------

customer_df.select(countDistinct('c_first_name').alias("unique_entries")).show()

# COMMAND ----------

item_df = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/item.dat")
display(item_df)

# COMMAND ----------

lowest_cost_item = item_df.select(min("i_wholesale_cost").alias("lowest_cost"))
lowest_cost_item.printSchema()
display(lowest_cost_item)

# COMMAND ----------

value = lowest_cost_item.select('lowest_cost').collect()[0]['lowest_cost']
min_cost_df = item_df.filter(item_df['i_wholesale_cost'] == value)
display(min_cost_df)

# COMMAND ----------

expression = item_df['i_wholesale_cost'] == lowest_cost_item['lowest_cost']
cheapest_item_df = item_df.join(lowest_cost_item, expression, "inner")
selecter_cheapest = cheapest_item_df.select("i_item_id", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_category", "i_manager_id")
display(selecter_cheapest)

# COMMAND ----------

highest_price = item_df.select(max('i_current_price').alias("highest_cost"))
highest_value = highest_price.select('highest_cost').collect()[0]['highest_cost']
print(highest_value)

# COMMAND ----------

max_cost_df = item_df.filter(item_df['i_current_price'] == highest_value)
display(max_cost_df)

# COMMAND ----------

highest_expression = item_df['i_current_price'] == highest_price['highest_cost']
highest_item_df = item_df.join(highest_price, highest_expression, 'inner')
highest_selected_df = highest_item_df.select("i_item_id", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_category", "i_manager_id")
display(highest_selected_df)

# COMMAND ----------

store_sales_df = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/store_sales.dat")
display(store_sales_df)

# COMMAND ----------

total_net_tax = store_sales_df.select(sum('ss_net_paid_inc_tax').alias("total"), sum('ss_net_profit').alias('profit'))
total_net_tax.show()

# COMMAND ----------

store_sales_df.count()

# COMMAND ----------

unique_quantity = store_sales_df.select(sumDistinct('ss_quantity').alias("quantity"), sum('ss_quantity').alias("quantityWithDuplicates"))
unique_quantity.show()

# COMMAND ----------

average_quantity = store_sales_df.select(avg('ss_quantity').alias('average'), mean('ss_quantity').alias("mean"), (sum('ss_quantity') / count('ss_quantity')).alias('calculated_average'), min('ss_quantity').alias('min'), max('ss_quantity').alias('max'))
average_quantity.show()

# COMMAND ----------

grouped_sold_items = store_sales_df.groupBy("ss_customer_sk").agg(countDistinct("ss_item_sk").alias("item_count"), sum("ss_quantity").alias("quantity"), round(sum("ss_net_paid"), 2).alias("total_net"), round(max("ss_net_paid"), 2).alias("max_paid"), round(min("ss_net_paid"), 2).alias("min_paid"), round(avg("ss_net_paid"), 2).alias("avg_paied")).withColumnRenamed("ss_customer_sk", "customer_id")
grouped_sold_items.show()
