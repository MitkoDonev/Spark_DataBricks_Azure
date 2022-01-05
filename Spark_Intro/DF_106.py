# Databricks notebook source
# MAGIC %fs ls /FileStore/tables/retailer/data

# COMMAND ----------

from pyspark.sql.functions import concat

# COMMAND ----------

customerDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/customer.dat")
customerDF.printSchema()

# COMMAND ----------

customerAddressDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/customer_address.dat")
customerAddressDF.printSchema()

# COMMAND ----------

householdDemographicsDF = spark.read.option("header", "true").option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/household_demographics.dat")
householdDemographicsDF.printSchema()

# COMMAND ----------

incomeBandDF = spark.read.option("sep", "|").option("inferSchema", 'true').csv("/FileStore/tables/retailer/data/income_band.dat")
incomeBandDF = incomeBandDF.withColumnRenamed("_c0", "ib_income_band_sk").withColumnRenamed("_c1", "ib_lower_bound").withColumnRenamed("_c2", "ib_upper_bound")
display(incomeBandDF)

# COMMAND ----------

first_condition = customerDF['c_current_hdemo_sk'] == householdDemographicsDF['hd_demo_sk']
second_condition = incomeBandDF['ib_income_band_sk'] == householdDemographicsDF['hd_income_band_sk']

joinedTablesDF = customerDF.join(householdDemographicsDF, first_condition, "inner").join(incomeBandDF, second_condition, "inner")
filteredTableDF = joinedTablesDF.filter((joinedTablesDF['ib_lower_bound'] >= 38128) & (joinedTablesDF['ib_upper_bound'] <= 88128))

third_condition = customerDF['c_current_addr_sk'] == customerAddressDF['ca_address_sk']

joinedAddressFilteredDF = filteredTableDF.join(customerAddressDF, third_condition, 'inner')
filterJoinedAdressDF = joinedAddressFilteredDF.filter(joinedAddressFilteredDF['ca_city'] == 'Edgewood')

# COMMAND ----------

selected_fields = filterJoinedAdressDF.select("c_customer_id", "c_customer_sk", concat(filterJoinedAdressDF['c_first_name'], filterJoinedAdressDF['c_last_name']), "ca_city", "ib_lower_bound", "ib_upper_bound")
formatedDF = selected_fields.withColumnRenamed("c_customer_id", "customer_id").withColumnRenamed("c_customer_sk", "customer_sk").withColumnRenamed("concat(c_first_name, c_last_name)", "full_name").withColumnRenamed("ca_city", "city").withColumnRenamed("ib_lower_bound", "lower_bound").withColumnRenamed("ib_upper_bound", "upper_bound")
formatedDF.printSchema()
