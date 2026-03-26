-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Insurance Claims Pipeline (SDP)
-- MAGIC
-- MAGIC Medallion architecture: **Bronze → Silver → Gold**
-- MAGIC with data quality expectations at each layer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer: Cleansed Claims

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_silver(
  CONSTRAINT valid_processing_days EXPECT (processing_days >= 0),
  CONSTRAINT valid_region EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_claim_id_format EXPECT (claim_id LIKE 'CLM-%'),
  CONSTRAINT positive_amount EXPECT (claim_amount > 0)
)
AS
SELECT
  claim_id,
  policy_id,
  customer_name,
  claim_type,
  claim_amount,
  claim_date,
  region,
  status,
  priority,
  processing_days,
  description,
  ingested_at,
  current_timestamp() AS processed_at
FROM LIVE.claims_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer: Claims Summary by Type and Region

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_gold
AS
SELECT
  claim_type,
  region,
  status,
  COUNT(*) AS claim_count,
  ROUND(SUM(claim_amount), 2) AS total_amount,
  ROUND(AVG(claim_amount), 2) AS avg_amount,
  ROUND(MIN(claim_amount), 2) AS min_amount,
  ROUND(MAX(claim_amount), 2) AS max_amount,
  ROUND(AVG(processing_days), 1) AS avg_processing_days,
  MIN(claim_date) AS earliest_claim,
  MAX(claim_date) AS latest_claim,
  current_timestamp() AS refreshed_at
FROM LIVE.claims_silver
GROUP BY claim_type, region, status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer: Monthly Claims Trend

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_monthly_trend
AS
SELECT
  DATE_TRUNC('month', claim_date) AS claim_month,
  claim_type,
  COUNT(*) AS claim_count,
  ROUND(SUM(claim_amount), 2) AS total_amount,
  ROUND(AVG(processing_days), 1) AS avg_processing_days
FROM LIVE.claims_silver
GROUP BY DATE_TRUNC('month', claim_date), claim_type
ORDER BY claim_month DESC, claim_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer: High-Value Claims (Priority Watch)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW claims_high_value
AS
SELECT
  claim_id,
  policy_id,
  customer_name,
  claim_type,
  claim_amount,
  region,
  status,
  priority,
  processing_days,
  claim_date
FROM LIVE.claims_silver
WHERE claim_amount > 100000
ORDER BY claim_amount DESC;
