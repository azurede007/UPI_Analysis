# Databricks notebook source
# =========================================================
# GOLD LAYER
# Business Aggregations / KPIs
# =========================================================

silver_txn_df = spark.table("upi_catalog.upi_database.silver_upi_transactions")
silver_fraud_df = spark.table("upi_catalog.upi_database.silver_fraud_analysis")


# COMMAND ----------

# =========================================================
# Gold Bank KPI
# =========================================================
from pyspark.sql.functions import *
gold_bank_kpi = silver_txn_df.groupBy("bank_id") \
    .agg(
        count("txn_id").alias("total_transactions"),
        round(sum("amount"),2).alias("total_amount"),
        sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
        round(avg("amount"), 2).alias("avg_transaction_amount")
    )
# gold_bank_kpi.display()
gold_bank_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("upi_catalog.upi_database.gold_bank_kpi")

print("Data Written into gold_bank_kpi table")


# COMMAND ----------

# =========================================================
# Gold Merchant KPI
# =========================================================

gold_merchant_kpi = silver_txn_df.groupBy(
        "merchant_id",
        "merchant_name",
        "merchant_category"
    ) \
    .agg(
        count("txn_id").alias("total_transactions"),
        round(sum("amount"),2).alias("total_revenue"),
        round(avg("amount"), 2).alias("avg_transaction_value")
    )
gold_merchant_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("upi_catalog.upi_database.gold_merchant_kpi")
    
print("Data Written into gold_merchant_kpi table")


# COMMAND ----------

# =========================================================
# Gold Fraud Monitoring
# =========================================================

gold_fraud_monitoring = silver_fraud_df.groupBy(
        "fraud_status",
        "fraud_type"
    ) \
    .agg(
        count("fraud_id").alias("fraud_count"),
        max("risk_score").alias("max_risk_score"),
        round(avg("risk_score"), 2).alias("avg_risk_score")
    )
gold_fraud_monitoring.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("upi_catalog.upi_database.gold_fraud_monitoring")
    
print("Data Written into gold_fraud_monitoring table")

# COMMAND ----------

# =========================================================
# Validation Queries
# =========================================================

spark.sql("""

SELECT * from
upi_catalog.upi_database.gold_fraud_monitoring

""").display()