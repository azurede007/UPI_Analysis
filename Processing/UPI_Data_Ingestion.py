# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Ingestion: Raw data into Bronze Table
# MAGIC - Transaction
# MAGIC - Settlement
# MAGIC - Merchant
# MAGIC - Fraud
# MAGIC - Device Log
# MAGIC - Customer

# COMMAND ----------

# ============================================
# LOAD DELTA TABLES INTO DATAFRAMES
# ============================================

upi_txn_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/upi_transactions_20260508.csv")

bank_settlement_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/bank_settlement_20260508.csv")

merchant_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/merchant_master.csv")

customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/customer_master.csv")

fraud_df = spark.read.format("json") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/fraud_logs.json")

device_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/Volumes/upi_catalog/upi_database/upi_rawdata/device_logs.csv")

# COMMAND ----------

# VALIDATE DATA
fraud_df = spark.read.format("json").load("/Volumes/upi_catalog/upi_database/upi_rawdata/fraud_logs.json")
display(fraud_df)
fraud_df.printSchema()

# COMMAND ----------

# ============================================
# WRITE DATAFRAMES INTO BRONZE TABLES
# ============================================

upi_txn_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_upi_transactions")
bank_settlement_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_bank_settlement")
merchant_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_merchant_master")
customer_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_customer_master")
fraud_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_fraud_logs")
device_df.write.mode("overwrite").saveAsTable("upi_catalog.upi_database.bronze_device_logs")  

print("Data Loaded into Bronze Tables")
