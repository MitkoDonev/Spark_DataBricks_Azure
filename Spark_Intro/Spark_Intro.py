# Databricks notebook source
customer = spark.read.option("header", "true").csv("/FileStore/tables/retailer/data/customer.csv")

# COMMAND ----------

display(customer)

# COMMAND ----------

customer.count()

# COMMAND ----------

sameBirthMonth = customer.filter(customer["c_birth_year"] == "1990").filter(customer["c_birth_month"] == "12").filter(customer["c_birth_day"] == "15")
sameBirthMonth.count()
display(sameBirthMonth)
