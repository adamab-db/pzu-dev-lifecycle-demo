# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Insurance Claims Data
# MAGIC
# MAGIC Generates synthetic insurance claims data and writes it to the bronze layer.
# MAGIC Simulates ingestion from an upstream source system.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
dbutils.widgets.text("schema", "dev_claims")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from faker import Faker
from pyspark.sql import functions as F
import random
from datetime import datetime

fake = Faker("pl_PL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Claims

# COMMAND ----------

CLAIM_TYPES = ["Motor", "Property", "Health", "Life", "Travel", "Liability"]
REGIONS = [
    "Mazowieckie", "Malopolskie", "Slaskie", "Wielkopolskie",
    "Dolnoslaskie", "Pomorskie", "Lodzkie", "Lubelskie",
]
STATUSES = ["Open", "In Review", "Approved", "Rejected", "Paid", "Closed"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]

AMOUNT_RANGES = {
    "Motor": (500, 150_000),
    "Property": (1_000, 500_000),
    "Health": (100, 50_000),
    "Life": (5_000, 1_000_000),
    "Travel": (100, 20_000),
    "Liability": (1_000, 200_000),
}

NUM_RECORDS = 5_000

records = []
for i in range(NUM_RECORDS):
    claim_type = random.choice(CLAIM_TYPES)
    min_amt, max_amt = AMOUNT_RANGES[claim_type]
    records.append({
        "claim_id": f"CLM-{i + 1:06d}",
        "policy_id": f"POL-{random.randint(1, 2000):06d}",
        "customer_name": fake.name(),
        "claim_type": claim_type,
        "claim_amount": round(random.uniform(min_amt, max_amt), 2),
        "claim_date": fake.date_between(start_date="-2y", end_date="today"),
        "region": random.choice(REGIONS),
        "status": random.choice(STATUSES),
        "priority": random.choice(PRIORITIES),
        "processing_days": random.randint(1, 90),
        "description": fake.sentence(nb_words=10),
        "ingested_at": datetime.now(),
    })

# COMMAND ----------

df = spark.createDataFrame(records)

(
    df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.claims_bronze")
)

print(f"Wrote {df.count()} claims to {catalog}.{schema}.claims_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data — Regions

# COMMAND ----------

regions_data = [
    {"region": "Mazowieckie", "region_manager": "Jan Kowalski", "cost_center": "CC-MAZ-001"},
    {"region": "Malopolskie", "region_manager": "Anna Nowak", "cost_center": "CC-MAL-002"},
    {"region": "Slaskie", "region_manager": "Piotr Wisniewski", "cost_center": "CC-SLA-003"},
    {"region": "Wielkopolskie", "region_manager": "Maria Kaminska", "cost_center": "CC-WLK-004"},
    {"region": "Dolnoslaskie", "region_manager": "Tomasz Lewandowski", "cost_center": "CC-DOL-005"},
    {"region": "Pomorskie", "region_manager": "Katarzyna Zielinska", "cost_center": "CC-POM-006"},
    {"region": "Lodzkie", "region_manager": "Marek Szymanski", "cost_center": "CC-LOD-007"},
    {"region": "Lubelskie", "region_manager": "Ewa Wojcik", "cost_center": "CC-LUB-008"},
]

regions_df = spark.createDataFrame(regions_data)
regions_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.regions_reference")
print(f"Wrote {regions_df.count()} regions to {catalog}.{schema}.regions_reference")
