# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Check with DQX
# MAGIC
# MAGIC Uses [Databricks Labs DQX](https://databrickslabs.github.io/dqx/) to validate
# MAGIC claims data after the pipeline runs. Demonstrates:
# MAGIC - Defining quality rules as code
# MAGIC - Applying checks with valid/invalid split (dead-letter pattern)
# MAGIC - Saving quarantined records for review

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
dbutils.widgets.text("schema", "dev_claims")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Quality Rules

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

checks = [
    # Completeness checks
    DQRowRule(
        name="claim_id_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="claim_id",
    ),
    DQRowRule(
        name="region_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="region",
    ),
    DQRowRule(
        name="claim_date_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="claim_date",
    ),
    # Validity checks
    DQRowRule(
        name="positive_claim_amount",
        criticality="error",
        check_func=check_funcs.is_in_range,
        column="claim_amount",
        check_func_kwargs={"min_limit": 0.01, "max_limit": 10_000_000},
    ),
    DQRowRule(
        name="valid_claim_type",
        criticality="error",
        check_func=check_funcs.is_in_list,
        column="claim_type",
        check_func_kwargs={"allowed": ["Motor", "Property", "Health", "Life", "Travel", "Liability"]},
    ),
    DQRowRule(
        name="valid_region",
        criticality="warn",
        check_func=check_funcs.is_in_list,
        column="region",
        check_func_kwargs={"allowed": [
            "Mazowieckie", "Malopolskie", "Slaskie", "Wielkopolskie",
            "Dolnoslaskie", "Pomorskie", "Lodzkie", "Lubelskie",
        ]},
    ),
    DQRowRule(
        name="non_negative_processing_days",
        criticality="warn",
        check_func=check_funcs.is_in_range,
        column="processing_days",
        check_func_kwargs={"min_limit": 0, "max_limit": 365},
    ),
    # Uniqueness check
    DQDatasetRule(
        name="claim_id_unique",
        criticality="error",
        check_func=check_funcs.is_unique,
        columns=["claim_id"],
    ),
]

print(f"Defined {len(checks)} quality rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Checks with Valid/Invalid Split

# COMMAND ----------

silver_df = spark.table(f"{catalog}.{schema}.claims_silver")
print(f"Silver table rows: {silver_df.count()}")

valid_df, invalid_df = dq_engine.apply_checks_and_split(silver_df, checks)

valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Valid rows:   {valid_count}")
print(f"Invalid rows: {invalid_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Quarantined Records

# COMMAND ----------

if invalid_count > 0:
    print("Sample quarantined records:")
    invalid_df.select("claim_id", "claim_type", "region", "claim_amount", "_error", "_warning").show(10, truncate=False)
else:
    print("No quarantined records — all data passed quality checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save valid records
(
    valid_df.drop("_warning", "_error")
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.claims_validated")
)
print(f"Saved {valid_count} valid records to {catalog}.{schema}.claims_validated")

# Save quarantined records for review
if invalid_count > 0:
    (
        invalid_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.claims_quarantine")
    )
    print(f"Saved {invalid_count} quarantined records to {catalog}.{schema}.claims_quarantine")
