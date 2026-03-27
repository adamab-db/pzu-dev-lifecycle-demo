# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Monitoring Setup
# MAGIC
# MAGIC Enables Lakehouse Monitoring on the gold tables for ongoing
# MAGIC drift detection and statistical profiling. Once enabled,
# MAGIC Databricks automatically generates metric tables and a quality dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_379d9b_catalog")
dbutils.widgets.text("schema", "dev_claims")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitors on Gold Tables

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

tables_to_monitor = [
    f"{catalog}.{schema}.claims_gold",
    f"{catalog}.{schema}.claims_monthly_trend",
]

for table_name in tables_to_monitor:
    try:
        existing = w.quality_monitors.get(table_name)
        print(f"Monitor already exists on {table_name} (status: {existing.status})")
    except Exception:
        print(f"Creating monitor on {table_name}...")
        w.quality_monitors.create(
            table_name=table_name,
            assets_dir=f"/Workspace/Users/adam.aboode@databricks.com/monitors/{schema}",
            output_schema_name=f"{catalog}.{schema}",
            snapshot={}  # snapshot profile type for non-time-series tables
        )
        print(f"Monitor created on {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Monitors
# MAGIC
# MAGIC Trigger an initial metric computation so the dashboard has data.

# COMMAND ----------

for table_name in tables_to_monitor:
    try:
        w.quality_monitors.run_refresh(table_name=table_name)
        print(f"Refresh triggered for {table_name}")
    except Exception as e:
        print(f"Refresh for {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Monitoring Results
# MAGIC
# MAGIC After the refresh completes, you can:
# MAGIC 1. **Catalog Explorer** → navigate to the table → **Monitor** tab
# MAGIC 2. **Auto-generated dashboard** in the assets directory
# MAGIC 3. **Query the metric tables** directly:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profile Metrics

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT table_name, column_name, data_type,
               num_nulls, num_distinct, min, max, mean
        FROM {catalog}.{schema}.claims_gold_profile_metrics
        ORDER BY column_name
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift Metrics (available after 2+ refreshes)

# COMMAND ----------

try:
    display(
        spark.sql(f"""
            SELECT table_name, column_name,
                   chi_squared_statistic, ks_statistic, js_distance
            FROM {catalog}.{schema}.claims_gold_drift_metrics
            WHERE chi_squared_statistic IS NOT NULL
            ORDER BY column_name
        """)
    )
except Exception as e:
    print(f"Drift metrics not yet available (need 2+ refreshes): {e}")
