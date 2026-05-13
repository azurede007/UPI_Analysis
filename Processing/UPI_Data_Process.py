# Databricks notebook source
# MAGIC %md
# MAGIC ### UPI Data Process

# COMMAND ----------

# =========================================================
# SILVER LAYER
# Cleansing + Enrichment
# =========================================================

bronze_txn = spark.table("upi_catalog.upi_database.bronze_upi_transactions")
bronze_settlement = spark.table("upi_catalog.upi_database.bronze_bank_settlement")
bronze_merchant = spark.table("upi_catalog.upi_database.bronze_merchant_master")
bronze_customer = spark.table("upi_catalog.upi_database.bronze_customer_master")
bronze_fraud = spark.table("upi_catalog.upi_database.bronze_fraud_logs")
bronze_device = spark.table("upi_catalog.upi_database.bronze_device_logs")

# COMMAND ----------

# =========================================================
# Silver Transaction Table
# =========================================================
from pyspark.sql.functions import *

silver_txn = bronze_txn.alias("t") \
    .join(bronze_settlement.alias("s"), "txn_id", "left") \
    .join(bronze_merchant.alias("m"), "merchant_id", "left") \
    .join(bronze_customer.alias("c"), "user_id", "left") \
    .join(bronze_device.alias("d"), "user_id", "left") \
    .select(
        col("txn_id"),
        col("user_id"),
        col("c.customer_name"),
        col("merchant_id"),
        col("m.merchant_name"),
        col("m.merchant_category"),
        col("s.bank_id"),
        col("amount"),
        col("status"),
        col("payment_mode"),
        col("txn_time"),
        col("s.settlement_amount"),
        col("s.settlement_status"),
        col("s.settlement_time"),
        col("c.city"),
        col("c.state"),
        col("c.kyc_status"),
        col("d.device_type"),
        col("d.os_version"),
        col("d.app_version")
    ) \
    .dropDuplicates(["txn_id"])

#Curation    
silver_df = silver_txn.na.fill({"bank_id": "NA", "settlement_amount": 0, "settlement_status": "NA"})
silver_curated = silver_df.withColumn("txn_hour",hour(col("txn_time"))).withColumn("txn_day",day(col("txn_time"))).withColumn("is_success",when(col("status")=="SUCCESS",1).otherwise(0))
#silver_curated.display()

silver_curated.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("upi_catalog.upi_database.silver_upi_transactions")

print("Data Written into silver_upi_transactions table")

# COMMAND ----------

silver_fraud = bronze_fraud.alias("f") \
    .join(bronze_txn.alias("t"), "txn_id", "left") \
    .join(bronze_customer.alias("c"), "user_id", "left") \
    .select(
        col("fraud_id"),
        col("txn_id"),
        col("c.user_id"),
        col("c.customer_name"),
        col("t.amount"),
        col("fraud_type"),
        col("risk_score"),
        col("fraud_status"),
        col("event_time")
    )
#silver_fraud.display()

silver_fraud.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("upi_catalog.upi_database.silver_fraud_analysis")
print("Data Written into silver_fraud_analysis table")