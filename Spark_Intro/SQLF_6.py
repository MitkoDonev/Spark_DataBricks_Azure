# Databricks notebook source
# MAGIC %sql
# MAGIC use retailer_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from date_dim limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from web_sales limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.warehouse
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/warehouse.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql select * from warehouse limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.web_site
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/web_site.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql select * from web_site limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists retailer_db.ship_mode
# MAGIC using csv
# MAGIC options(path "/FileStore/tables/retailer/data/ship_mode.dat",
# MAGIC sep "|", header true, inferSchema true)

# COMMAND ----------

# MAGIC %sql select * from ship_mode limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC w_warehouse_name as warehouse_name,
# MAGIC sm_type as shipping_type,
# MAGIC web_name as website_name,
# MAGIC sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1 else 0 end) as 30_days,
# MAGIC sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0) as 31_to_60_days,
# MAGIC sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0) as 61_to_90_days,
# MAGIC sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0) as 91_to_120_days,
# MAGIC sum(case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0) as over_120_days
# MAGIC from web_sales
# MAGIC inner join date_dim on ws_ship_date_sk = d_date_sk
# MAGIC inner join warehouse on ws_warehouse_sk = w_warehouse_sk
# MAGIC inner join ship_mode on ws_ship_mode_sk = sm_ship_mode_sk
# MAGIC inner join web_site on ws_web_site_sk = web_site_sk
# MAGIC where d_month_seq between 1200 and 1211
# MAGIC and w_warehouse_name is not null
# MAGIC group by warehouse_name, shipping_type, website_name
# MAGIC order by warehouse_name, shipping_type, website_name
