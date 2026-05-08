# ============================================
# READ DATA FROM DELTA LAKE INTO DATAFRAME
# ============================================

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UPI Delta Lake Processing") \
    .getOrCreate()

# ============================================
# LOAD DELTA TABLES INTO DATAFRAMES
# ============================================

upi_txn_df = spark.read.format("delta") \
    .load("/mnt/upi/bronze/upi_transactions")

bank_settlement_df = spark.read.format("delta") \
    .load("/mnt/upi/bronze/bank_settlement")

merchant_df = spark.read.format("delta") \
    .load("/mnt/upi/master/merchant_master")

customer_df = spark.read.format("delta") \
    .load("/mnt/upi/master/customer_master")

fraud_df = spark.read.format("delta") \
    .load("/mnt/upi/bronze/fraud_logs")

device_df = spark.read.format("delta") \
    .load("/mnt/upi/bronze/device_logs")

# ============================================
# DISPLAY DATA
# ============================================

upi_txn_df.show(5)

# ============================================
# TRANSFORMATIONS
# ============================================

from pyspark.sql.functions import *

silver_df = upi_txn_df \
    .join(customer_df, "user_id", "left") \
    .join(merchant_df, "merchant_id", "left")

silver_df = silver_df.withColumn(
    "txn_hour",
    hour(col("txn_time"))
)

silver_df = silver_df.withColumn(
    "txn_day",
    dayofmonth(col("txn_time"))
)

silver_df = silver_df.withColumn(
    "txn_month",
    month(col("txn_time"))
)

silver_df = silver_df.withColumn(
    "txn_year",
    year(col("txn_time"))
)

silver_df = silver_df.withColumn(
    "is_success",
    when(col("status") == "SUCCESS", 1).otherwise(0)
)

# ============================================
# WRITE DATAFRAME INTO DELTA TABLE
# ============================================

silver_df.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/upi/silver/silver_upi_transactions")

# ============================================
# CREATE GOLD AGGREGATION
# ============================================

gold_bank_kpi_df = silver_df.groupBy("bank_id") \
    .agg(
        count("*").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum("is_success").alias("success_transactions")
    )

gold_bank_kpi_df = gold_bank_kpi_df.withColumn(
    "success_rate",
    (col("success_transactions") / col("total_transactions")) * 100
)

# ============================================
# WRITE GOLD DATA INTO DELTA TABLE
# ============================================

gold_bank_kpi_df.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/upi/gold/gold_bank_kpi")

# ============================================
# READ WRITTEN GOLD TABLE
# ============================================

final_gold_df = spark.read.format("delta") \
    .load("/mnt/upi/gold/gold_bank_kpi")

final_gold_df.show()

# ============================================
# SAVE AS MANAGED DELTA TABLE
# ============================================

final_gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_bank_kpi")

# ============================================
# VERIFY TABLE
# ============================================

spark.sql("SELECT * FROM gold_bank_kpi").show()
# ============================================