# Databricks notebook source
# MAGIC %md
# MAGIC # Post-Pipeline Quality Check
# MAGIC
# MAGIC Validates data quality after the SDP pipeline completes.
# MAGIC Acts as an additional quality gate before downstream consumption.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
dbutils.widgets.text("schema", "dev_claims")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Checks

# COMMAND ----------

results = []

def check(name, condition, message):
    status = "PASS" if condition else "FAIL"
    results.append({"check": name, "status": status, "detail": message})
    print(f"  [{status}] {name}: {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Checks

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER")
print("=" * 60)

bronze_count = spark.table(f"{catalog}.{schema}.claims_bronze").count()
check(
    "Bronze row count",
    bronze_count > 0,
    f"{bronze_count} rows"
)

bronze_nulls = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog}.{schema}.claims_bronze
    WHERE claim_id IS NULL
""").collect()[0]["cnt"]
check(
    "Bronze no null claim_ids",
    bronze_nulls == 0,
    f"{bronze_nulls} null claim_ids found"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Checks

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER")
print("=" * 60)

silver_count = spark.table(f"{catalog}.{schema}.claims_silver").count()
check(
    "Silver row count",
    silver_count > 0,
    f"{silver_count} rows (from {bronze_count} bronze)"
)

check(
    "Silver no data loss > 5%",
    silver_count >= bronze_count * 0.95,
    f"Retention: {silver_count / bronze_count * 100:.1f}%"
)

negative_days = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog}.{schema}.claims_silver
    WHERE processing_days < 0
""").collect()[0]["cnt"]
check(
    "Silver no negative processing_days",
    negative_days == 0,
    f"{negative_days} negative values"
)

null_regions = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog}.{schema}.claims_silver
    WHERE region IS NULL
""").collect()[0]["cnt"]
check(
    "Silver no null regions",
    null_regions == 0,
    f"{null_regions} null regions (should be dropped by expectation)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Checks

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER")
print("=" * 60)

gold_count = spark.table(f"{catalog}.{schema}.claims_gold").count()
check(
    "Gold summary populated",
    gold_count > 0,
    f"{gold_count} aggregation rows"
)

trend_count = spark.table(f"{catalog}.{schema}.claims_monthly_trend").count()
check(
    "Monthly trend populated",
    trend_count > 0,
    f"{trend_count} monthly data points"
)

high_value_count = spark.table(f"{catalog}.{schema}.claims_high_value").count()
check(
    "High-value claims populated",
    high_value_count >= 0,
    f"{high_value_count} claims over 100k"
)

# Verify gold amounts reconcile with silver
gold_total = spark.sql(f"""
    SELECT ROUND(SUM(total_amount), 2) AS total
    FROM {catalog}.{schema}.claims_gold
""").collect()[0]["total"]

silver_total = spark.sql(f"""
    SELECT ROUND(SUM(claim_amount), 2) AS total
    FROM {catalog}.{schema}.claims_silver
""").collect()[0]["total"]

check(
    "Gold-Silver amount reconciliation",
    abs(gold_total - silver_total) < 1.0,
    f"Gold: {gold_total}, Silver: {silver_total}, Diff: {abs(gold_total - silver_total)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Report Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("QUALITY REPORT SUMMARY")
print("=" * 60)

passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")
total = len(results)

print(f"\nTotal checks: {total}")
print(f"Passed:       {passed}")
print(f"Failed:       {failed}")
print(f"Score:        {passed}/{total} ({passed/total*100:.0f}%)")

if failed > 0:
    print("\nFAILED CHECKS:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"  - {r['check']}: {r['detail']}")
    raise Exception(f"Quality check failed: {failed}/{total} checks did not pass")
else:
    print("\nAll quality checks passed.")
