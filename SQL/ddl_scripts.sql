-- ============================================
-- CREATE TABLE STATEMENTS FOR ALL SOURCE FILES
-- ============================================

-- 1. UPI TRANSACTIONS TABLE
CREATE TABLE upi_catalog.upi_database.bronze_upi_transactions (
    txn_id STRING,
    user_id STRING,
    merchant_id STRING,
    bank_id STRING,
    amount DOUBLE,
    status STRING,
    payment_mode STRING,
    txn_time TIMESTAMP
)
USING DELTA;

-- ============================================

-- 2. BANK SETTLEMENT TABLE
CREATE TABLE upi_catalog.upi_database.bronze_bank_settlement (
    settlement_id STRING,
    txn_id STRING,
    bank_id STRING,
    settlement_amount DOUBLE,
    settlement_status STRING,
    settlement_time TIMESTAMP
)
USING DELTA;

-- ============================================

-- 3. MERCHANT MASTER TABLE
CREATE TABLE upi_catalog.upi_database.bronze_merchant_master (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    city STRING,
    state STRING
)
USING DELTA;

-- ============================================

-- 4. CUSTOMER MASTER TABLE
CREATE TABLE upi_catalog.upi_database.bronze_customer_master (
    user_id STRING,
    customer_name STRING,
    age INT,
    gender STRING,
    city STRING,
    state STRING,
    kyc_status STRING
)
USING DELTA;

-- ============================================

-- 5. FRAUD LOGS TABLE
Drop table if exists upi_catalog.upi_database.bronze_fraud_logs;
CREATE TABLE upi_catalog.upi_database.bronze_fraud_logs (
    fraud_id STRING,
    txn_id STRING,
    fraud_type STRING,
    risk_score INT,
    fraud_status STRING,
    event_time TIMESTAMP
)
USING DELTA;

-- ============================================

-- 6. DEVICE LOGS TABLE
CREATE TABLE upi_catalog.upi_database.bronze_device_logs (
    log_id STRING,
    user_id STRING,
    device_type STRING,
    os_version STRING,
    app_version STRING,
    ip_address STRING,
    login_time TIMESTAMP
)
USING DELTA;

-- ============================================