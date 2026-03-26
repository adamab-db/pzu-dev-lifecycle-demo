# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests: Claims Transformations
# MAGIC
# MAGIC pytest-style tests validating transformation logic
# MAGIC and business rules for the claims pipeline.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
dbutils.widgets.text("schema", "dev_claims")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Setup

# COMMAND ----------

import json

test_results = []

def test(name):
    """Simple test decorator that captures pass/fail."""
    def decorator(func):
        try:
            func()
            test_results.append({"test": name, "status": "PASSED"})
            print(f"  PASSED: {name}")
        except AssertionError as e:
            test_results.append({"test": name, "status": "FAILED", "error": str(e)})
            print(f"  FAILED: {name} — {e}")
        except Exception as e:
            test_results.append({"test": name, "status": "ERROR", "error": str(e)})
            print(f"  ERROR:  {name} — {e}")
    return decorator

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Validation Tests

# COMMAND ----------

print("=" * 50)
print("SCHEMA VALIDATION")
print("=" * 50)

@test("Bronze table has expected columns")
def _():
    cols = set(spark.table(f"{catalog}.{schema}.claims_bronze").columns)
    expected = {"claim_id", "policy_id", "customer_name", "claim_type",
                "claim_amount", "claim_date", "region", "status",
                "priority", "processing_days", "description", "ingested_at"}
    missing = expected - cols
    assert not missing, f"Missing columns: {missing}"

@test("Silver table has processed_at column")
def _():
    cols = spark.table(f"{catalog}.{schema}.claims_silver").columns
    assert "processed_at" in cols, "processed_at column missing from silver"

@test("Gold summary has aggregation columns")
def _():
    cols = set(spark.table(f"{catalog}.{schema}.claims_gold").columns)
    expected = {"claim_count", "total_amount", "avg_amount", "avg_processing_days"}
    missing = expected - cols
    assert not missing, f"Missing columns: {missing}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Type Tests

# COMMAND ----------

print("=" * 50)
print("DATA TYPE VALIDATION")
print("=" * 50)

@test("claim_amount is numeric")
def _():
    dtype = dict(spark.table(f"{catalog}.{schema}.claims_bronze").dtypes)["claim_amount"]
    assert dtype == "double", f"Expected double, got {dtype}"

@test("claim_date is date type")
def _():
    dtype = dict(spark.table(f"{catalog}.{schema}.claims_bronze").dtypes)["claim_date"]
    assert dtype == "date", f"Expected date, got {dtype}"

@test("processing_days is integer")
def _():
    dtype = dict(spark.table(f"{catalog}.{schema}.claims_bronze").dtypes)["processing_days"]
    assert "int" in dtype.lower(), f"Expected int, got {dtype}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Rule Tests

# COMMAND ----------

print("=" * 50)
print("BUSINESS RULES")
print("=" * 50)

@test("All claim_ids follow CLM-XXXXXX pattern")
def _():
    bad = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.claims_silver
        WHERE claim_id NOT LIKE 'CLM-%'
    """).collect()[0]["cnt"]
    assert bad == 0, f"{bad} claim_ids don't match pattern"

@test("All claim amounts are positive in silver")
def _():
    bad = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.claims_silver
        WHERE claim_amount <= 0
    """).collect()[0]["cnt"]
    assert bad == 0, f"{bad} non-positive amounts found"

@test("Claim types are from allowed set")
def _():
    allowed = {"Motor", "Property", "Health", "Life", "Travel", "Liability"}
    actual = {r["claim_type"] for r in spark.sql(f"""
        SELECT DISTINCT claim_type FROM {catalog}.{schema}.claims_silver
    """).collect()}
    unexpected = actual - allowed
    assert not unexpected, f"Unexpected claim types: {unexpected}"

@test("No future claim dates")
def _():
    future = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.claims_silver
        WHERE claim_date > current_date()
    """).collect()[0]["cnt"]
    assert future == 0, f"{future} claims have future dates"

@test("Gold totals reconcile with silver")
def _():
    gold = spark.sql(f"SELECT SUM(total_amount) AS t FROM {catalog}.{schema}.claims_gold").collect()[0]["t"]
    silver = spark.sql(f"SELECT SUM(claim_amount) AS t FROM {catalog}.{schema}.claims_silver").collect()[0]["t"]
    diff = abs(gold - silver)
    assert diff < 1.0, f"Reconciliation gap: {diff}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("TEST SUMMARY")
print("=" * 50)

passed = sum(1 for t in test_results if t["status"] == "PASSED")
failed = sum(1 for t in test_results if t["status"] == "FAILED")
errors = sum(1 for t in test_results if t["status"] == "ERROR")
total = len(test_results)

print(f"\nTotal:  {total}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print(f"Errors: {errors}")

if failed + errors > 0:
    print("\nFailing tests:")
    for t in test_results:
        if t["status"] != "PASSED":
            print(f"  [{t['status']}] {t['test']}: {t.get('error', '')}")
    raise Exception(f"{failed + errors} test(s) did not pass")
else:
    print("\nAll tests passed.")
