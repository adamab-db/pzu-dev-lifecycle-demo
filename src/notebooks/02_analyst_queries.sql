-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Business Analyst Queries
-- MAGIC
-- MAGIC Sample analytical queries for the business analyst persona.
-- MAGIC These demonstrate how analysts consume data produced by the engineering team.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
-- MAGIC dbutils.widgets.text("schema", "dev_claims")
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Top Claim Categories by Total Amount

-- COMMAND ----------

SELECT
  claim_type,
  SUM(claim_count) AS total_claims,
  ROUND(SUM(total_amount), 2) AS total_payout,
  ROUND(SUM(total_amount) / SUM(claim_count), 2) AS avg_per_claim
FROM claims_gold
GROUP BY claim_type
ORDER BY total_payout DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Regional Performance Dashboard

-- COMMAND ----------

SELECT
  region,
  SUM(claim_count) AS total_claims,
  ROUND(SUM(total_amount), 2) AS total_payout,
  ROUND(AVG(avg_processing_days), 1) AS avg_days_to_process
FROM claims_gold
GROUP BY region
ORDER BY total_payout DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Monthly Claims Trend

-- COMMAND ----------

SELECT
  claim_month,
  SUM(claim_count) AS total_claims,
  ROUND(SUM(total_amount), 2) AS total_amount
FROM claims_monthly_trend
GROUP BY claim_month
ORDER BY claim_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## High-Value Claims Requiring Attention

-- COMMAND ----------

SELECT
  claim_id,
  customer_name,
  claim_type,
  claim_amount,
  region,
  status,
  priority,
  processing_days
FROM claims_high_value
WHERE status IN ('Open', 'In Review')
ORDER BY claim_amount DESC
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Claims Status Distribution

-- COMMAND ----------

SELECT
  status,
  SUM(claim_count) AS count,
  ROUND(SUM(total_amount), 2) AS total_value
FROM claims_gold
GROUP BY status
ORDER BY count DESC
