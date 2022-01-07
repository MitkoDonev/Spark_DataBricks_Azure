# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer limit 10

# COMMAND ----------

# MAGIC %sql select * from customerAddress limit 10 

# COMMAND ----------

# MAGIC %sql select * from item limit 10

# COMMAND ----------

# MAGIC %sql select * from date_dim limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.web_sales
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/web_sales.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql select * from web_sales limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ca.ca_zip,
# MAGIC ca.ca_city,
# MAGIC ca.ca_country,
# MAGIC ca.ca_state,
# MAGIC sum(ws.ws_sales_price) total_web_sales_price
# MAGIC from web_sales ws
# MAGIC inner join customer c on c.c_customer_sk = ws.ws_bill_customer_sk
# MAGIC inner join customerAddress ca on ca.ca_address_sk = c.c_current_addr_sk
# MAGIC inner join item it on it.i_item_sk = ws.ws_item_sk
# MAGIC inner join date_dim dt on dt.d_date_sk = ws.ws_sold_date_sk
# MAGIC where dt.d_year = 2001 and dt.d_qoy = 2 and (substr(ca.ca_zip,1,5) in ('85669', '86197', '86475', '85392', '85460', '80348', '81792')
# MAGIC or it.i_item_id in ( select i_item_id from item where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)))
# MAGIC group by ca.ca_zip, ca.ca_city, ca.ca_country, ca.ca_state
# MAGIC order by total_web_sales_price desc
