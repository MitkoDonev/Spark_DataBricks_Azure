# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

from pyspark.sql.functions import expr, col

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select c_customer_sk, c_salutation, c_first_name, c_last_name from retailer_db.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC c_customer_sk,
# MAGIC c_salutation,
# MAGIC upper(c_first_name),
# MAGIC c_first_name,
# MAGIC c_last_name
# MAGIC from retailer_db.customer

# COMMAND ----------

customerAddressDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", "true").csv("/FileStore/tables/retailer/data/customer_address.dat")
display(customerAddressDF)

# COMMAND ----------

customerAddressDF.select(col("ca_address_sk").alias("ca_id"), expr("ca_address_sk as id")).show()

# COMMAND ----------

validAddresses = customerAddressDF.select(col("ca_address_sk"), col("ca_street_name"), col("ca_street_number"), expr("ca_street_number is not null and length(ca_street_number) > 0 as isValidAddress"))
display(validAddresses)

# COMMAND ----------

customerAddressDF.selectExpr("ca_address_sk", "upper(ca_street_name) as street_name", "ca_street_number", "ca_street_number is not null and length(ca_street_number) > 0 as isValidAddress").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select length(c_salutation) from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer where c_birth_month = 1 and lower(c_birth_country) = "canada" and c_birth_year != 1980 and (trim(lower(c_salutation)) == "miss" or trim(lower(c_salutation)) == "mrs.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select c_customer_sk, c_birth_country, c_first_name, c_last_name from customer where c_birth_country is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(c_first_name) from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer where c_first_name is null

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.store_sales
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/store_sales.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store_sales limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(ss_quantity) from store_sales 

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(ss_net_paid) from store_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store_sales where ss_net_paid = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(ss_net_paid) from store_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store_sales where ss_net_paid = 19562.4

# COMMAND ----------

# MAGIC %sql
# MAGIC select ss_store_sk, sum(ss_net_profit) from store_sales where ss_store_sk is not null group by ss_store_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC select ss_store_sk, ss_item_sk, count(ss_quantity), sum(ss_net_profit) from store_sales
# MAGIC where ss_store_sk is not null and ss_item_sk is not null
# MAGIC group by ss_store_sk, ss_item_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC select c_birth_year, c_birth_country, count(*) as c_count
# MAGIC from customer
# MAGIC where c_birth_country is not null and c_birth_year is not null
# MAGIC group by c_birth_year, c_birth_country
# MAGIC having count(*) > 10 and c_birth_country like "A%" and c_birth_year > 1980
# MAGIC order by c_count, c_birth_year desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ca_city,
# MAGIC count(*)
# MAGIC from customer c
# MAGIC inner join customerAddress ca
# MAGIC on c.c_current_addr_sk == ca.ca_address_sk
# MAGIC where ca_city is not null
# MAGIC group by ca_city order by count(*) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct ss_customer_sk) from store_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct c_customer_sk) from customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC c_customer_sk customerID,
# MAGIC concat(c_first_name, c_last_name) fullName,
# MAGIC ss_customer_sk customerID_Store,
# MAGIC ss_item_sk itemID
# MAGIC from customer c
# MAGIC left outer join store_sales s
# MAGIC on c_customer_sk = s.ss_customer_sk
# MAGIC where s.ss_customer_sk is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC c_customer_sk customerID,
# MAGIC concat(c_first_name, c_last_name) fullName,
# MAGIC ss_customer_sk customerID_Store,
# MAGIC ss_item_sk itemID
# MAGIC from customer c
# MAGIC right outer join store_sales s
# MAGIC on c_customer_sk = s.ss_customer_sk
# MAGIC where s.ss_customer_sk is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC c_customer_sk,
# MAGIC c_first_name,
# MAGIC c_last_name,
# MAGIC c_birth_year,
# MAGIC c_birth_country
# MAGIC from customer
# MAGIC where c_birth_country in ("FIJI", "TOGO", "SURINAME")
# MAGIC and c_birth_year between 1960 and 1980
# MAGIC and c_last_name like "M%"
# MAGIC and c_first_name like "_e%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC min(ss_sales_price),
# MAGIC avg(ss_sales_price),
# MAGIC max(ss_sales_price)
# MAGIC from store_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ss_item_sk,
# MAGIC ss_sales_price,
# MAGIC ss_customer_sk,
# MAGIC case
# MAGIC when ss_sales_price between 0 and 37.89235310305823 then "belowAVG"
# MAGIC when ss_sales_price between 37.89235310305823 and 199.56 then "aboveAVG"
# MAGIC else "unknown" end as priceCategory
# MAGIC from store_sales
