# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.catalog_sales
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/catalog_sales.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.item
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/item.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.date_dim
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/date_dim.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC it.i_manufact_id manufactID,
# MAGIC sum(cs_ext_discount_amt) excess_discount_amount
# MAGIC from catalog_sales cs
# MAGIC inner join item it on it.i_item_sk == cs.cs_item_sk
# MAGIC inner join date_dim dt on dt.d_date_sk == cs.cs_sold_date_sk
# MAGIC where it.i_manufact_id = 977
# MAGIC and dt.d_date between date("2000-01-27") and date_add("2000-01-27", 90)
# MAGIC and cs.cs_ext_discount_amt > (
# MAGIC   select 1.3 * avg(cs_ext_discount_amt)
# MAGIC   from catalog_sales
# MAGIC   inner join date_dim on d_date_sk == cs_sold_date_sk
# MAGIC   where cs_item_sk == i_item_sk
# MAGIC   and d_date between date("2000-01-27") and date_add("2000-01-27", 90)
# MAGIC )
# MAGIC group by it.i_manufact_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select avg(cs_ext_discount_amt) from catalog_sales
