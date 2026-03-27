# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Risk Scoring (Python SDP)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F


@dlt.table(comment="Claims with risk score")
@dlt.expect("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "claim_amount > 0")
def claims_risk_scored():
    return (
        dlt.read("claims_silver")
        .withColumn(
            "risk_score",
            F.when(F.col("claim_amount") > 100_000, "High")
            .when(F.col("claim_amount") > 10_000, "Medium")
            .otherwise("Low"),
        )
    )
