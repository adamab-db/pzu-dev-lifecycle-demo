"""
Explore claims data using Databricks Connect.

Run this script locally in VS Code — it executes Spark queries
on the remote Databricks cluster via Databricks Connect.

Demo talking point: "I'm running this in VS Code on my laptop,
but the Spark execution happens on the Databricks serverless compute.
Same code works locally and in a notebook."
"""

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("fevm-serverless-stable-379d9b").serverless(True).getOrCreate()

CATALOG = "serverless_stable_379d9b_catalog"
SCHEMA = "dev_claims"

# Quick look at the data
print("=== Claims Bronze (first 5 rows) ===")
spark.table(f"{CATALOG}.{SCHEMA}.claims_bronze").show(5, truncate=False)

# Summary stats
print("=== Claims by Type ===")
spark.sql(f"""
    SELECT claim_type, COUNT(*) AS count, ROUND(AVG(claim_amount), 2) AS avg_amount
    FROM {CATALOG}.{SCHEMA}.claims_bronze
    GROUP BY claim_type
    ORDER BY count DESC
""").show()

# Reuse the transformation functions from helpers
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from helpers.transformations import clean_claims, filter_high_value

raw = spark.table(f"{CATALOG}.{SCHEMA}.claims_bronze")
cleaned = clean_claims(raw)
high_value = filter_high_value(cleaned)

print(f"=== High-Value Claims (>{100_000:,}) ===")
print(f"Total: {high_value.count()}")
high_value.select("claim_id", "customer_name", "claim_type", "claim_amount", "region").show(10)
