# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

customerAddressDF = spark.read.option("header", "true").option("sep", "|").option("inferSchmea", "true").csv("/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF.printSchema()

# COMMAND ----------

customerAddressDF.createTempView("vCustomerAddress")

# COMMAND ----------

spark.sql("select * from vCustomerAddress").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vCustomerAddress

# COMMAND ----------

customerAddressDF.select("ca_address_sk", "ca_country", "ca_state", "ca_city", "ca_street_name").filter(customerAddressDF["ca_state"].contains("AK")).createTempView("vAK_Addresses")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vAK_Addresses

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gvCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC create database retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table retailer_db.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.customer(c_customer_sk LONG, c_customer_id STRING, c_current_cdemo_sk LONG, c_current_hdemo_sk LONG, c_current_addr_sk LONG, c_first_shipto_date_sk LONG, c_first_sales_date_sk LONG, c_salutation STRING, c_first_name STRING, c_last_name STRING, c_preferred_cust_flag STRING, c_birth_day INT, c_birth_month INT, c_birth_year INT, c_birth_country STRING, c_login STRING, c_email_address STRING, c_last_review_date LONG)
# MAGIC using csv
# MAGIC options(path '/FileStore/tables/retailer/data/customer.dat', sep "|", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC describe retailer_db.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retailer_db

# COMMAND ----------

customerAddressDF.write.saveAsTable("retailer_db.customerAddress")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retailer_db.customerWithAddress(customer_id LONG, fiest_name STRING, last_name STRING, country STRING, city STRING, state STRING, street_name STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into retailer_db.customerWithAddress
# MAGIC select c_customer_sk, c_first_name, c_last_name, ca_country, ca_city, ca_state, ca_street_name
# MAGIC from retailer_db.customer c
# MAGIC inner join customerAddress ca on ca.ca_address_sk = c.c_current_addr_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retailer_db.customerWithAddress
