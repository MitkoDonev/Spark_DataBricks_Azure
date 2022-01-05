# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

customer_df = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/customer.dat")
display(customer_df)

# COMMAND ----------

customerWithValidDate = customer_df.filter((customer_df['c_birth_day'] >= 1) & (customer_df['c_birth_day'] <= 31)).filter((customer_df['c_birth_month'] >= 1) & (customer_df['c_birth_month'] <= 12)).filter((customer_df['c_birth_year'] >= 1) & (customer_df['c_birth_year'].isNotNull()))

# COMMAND ----------

display(customerWithValidDate)

# COMMAND ----------

result = customerWithValidDate.where(customerWithValidDate['c_birth_day'] != 1)
display(result)
